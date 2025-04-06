from __future__ import annotations

__all__ = [
    'make_datapoint',
    'make_batch_update_record',
    'Index',
    'StreamIndex',
    'BatchIndex',
    'Endpoint',
    'DeployedIndex',
    'IndexDatapoint',
]

import functools
import logging
import time
from collections.abc import Sequence
from typing import Any, Literal
from uuid import uuid4

from google.cloud import aiplatform
from google.cloud.aiplatform import matching_engine
from typing_extensions import Self

from cloudly.gcp.storage import GcsBlobUpath
from cloudly.util.datetime import utcnow
from cloudly.util.serializer import NewlineDelimitedOrjsonSeriealizer

logger = logging.getLogger(__name__)

IndexDatapoint = aiplatform.compat.types.matching_engine_index.IndexDatapoint
Namespace = matching_engine.matching_engine_index_endpoint.Namespace
NumericNamespace = matching_engine.matching_engine_index_endpoint.NumericNamespace
MatchingEngineIndex = matching_engine.MatchingEngineIndex
MatchingEngineIndexEndpoint = matching_engine.MatchingEngineIndexEndpoint


def make_datapoint(
    id_: str, embedding: list[float], properties: dict[str, Any] | None = None
) -> IndexDatapoint:
    # TODO: look for a more suitable name for `properties`.
    restricts = []
    numeric_restricts = []
    if properties:
        for namespace, value in properties.items():
            assert isinstance(namespace, str)
            if isinstance(value, str):
                restricts.append(
                    IndexDatapoint.Restriction(namespace=namespace, allow_list=[value])
                )
            elif isinstance(value, list) and all(isinstance(v, str) for v in value):
                restricts.append(
                    IndexDatapoint.Restriction(namespace=namespace, allow_list=value)
                )
            elif isinstance(value, (int, float)):
                numeric_restricts.append(
                    IndexDatapoint.NumericRestriction(
                        namespace=namespace,
                        value_float=value,
                    )
                )
            else:
                raise ValueError(
                    f"values of type {type(value)} (at key '{namespace}') are not allowed as datapoint properties"
                )
    return IndexDatapoint(
        datapoint_id=id_,
        feature_vector=embedding,
        restricts=restricts,
        numeric_restricts=numeric_restricts,
    )


def make_batch_update_record(datapoint: IndexDatapoint) -> dict:
    return {
        'id': datapoint.datapoint_id,
        'embedding': [component for component in datapoint.feature_vector],
        'restricts': [
            {
                'namespace': restrict.namespace,
                'allow': [item for item in restrict.allow_list],
            }
            for restrict in datapoint.restricts
        ],
        'numeric_restricts': [
            {'namespace': restrict.namespace, 'value_float': restrict.value_float}
            for restrict in datapoint.numeric_restricts
        ],
    }


class Index:
    """
    The class `Index` is responsible for storing and managing (add/delete/update) data,
    as well as maintaining the "index" to facilitate similarity search.

    This base class does not have methods for adding data to the index.
    Usually user should use a subclass, either `StreamIndex` or `BatchIndex`.
    """

    def __init__(self, name: str):
        """
        `name` is either a fully qualified "resource name"
        (like 'projects/my-project-id/locations/us-west1/indexes/1234567890')
        or ID (like '1234567890', shown on GCP dashboard).

        If it's ID, then `init_global_config` must have been called with the correct `region` value.
        """
        self.name = name
        self._display_name = None
        self._resource_name = None
        self._config = None
        self._dimensions = None

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.name}')"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.name,)

    @property
    def index(self) -> MatchingEngineIndex:
        # `self.index` has some useful attributes directly accessible as instance attributes.
        # Some of these attributes are cached on `self` because they won't change.
        index = MatchingEngineIndex(self.name)
        self._display_name = index.display_name
        self._resource_name = index.resource_name
        self._config = index.to_dict()['metadata']['config']
        self._dimensions = int(self._config['dimensions'])
        return index

    @property
    def display_name(self) -> str:
        if self._display_name is None:
            self.index
        return self._display_name

    @property
    def resource_name(self) -> str:
        if self._resource_name is None:
            self.index
        return self._resource_name

    @property
    def config(self) -> dict:
        if self._config is None:
            self.index
        return self._config

    @property
    def dimensions(self) -> int:
        if self._dimensions is None:
            self.index
        return self._dimensions

    def __len__(self) -> int | None:
        # Number of datapoints in the index.
        # TODO: why could this return `None`?
        # TODO: is `__len__` suitable if the return value can be `None`?
        while True:
            n = self.index.to_dict()['indexStats'].get('vectorsCount')
            if n is not None:
                break
            time.sleep(0.1)
        return int(n)

    def remove_datapoints(self, datapoint_ids: Sequence[str]) -> Self:
        self.index.remove_datapoints(datapoint_ids)
        return self

    def deploy(self, endpoint_name: str, **kwargs) -> DeployedIndex:
        return Endpoint(endpoint_name).deploy_index(self, **kwargs)

    @property
    def deployed_indexes(self) -> list[dict]:
        return [
            {
                'endpoint_name': di.index_endpoint,  # resource_name
                'deployed_index_id': di.deployed_index_id,
                'display_name': di.display_name,
            }
            for di in self.index.deployed_indexes
        ]

    def undeploy(self, endpoint_name: str, deployed_index_id: str) -> None:
        Endpoint(endpoint_name).undeploy_index(deployed_index_id)

    def undeploy_all(self) -> None:
        for zz in self.deployed_indexes:
            self.undeploy(zz['endpoint_name'], zz['deployed_index_id'])

    def delete(self) -> None:
        """
        Delete this index (permanently).
        """
        self.index.delete()


class StreamIndex(Index):
    @classmethod
    def new(
        cls,
        display_name: str,
        *,
        dimensions: int,
        brute_force: bool = False,
        approximate_neighbors_count: int | None = None,
        **kwargs,
    ) -> Index:
        if not brute_force:
            assert approximate_neighbors_count
            index = MatchingEngineIndex.create_tree_ah_index(
                display_name=display_name,
                dimensions=dimensions,
                approximate_neighbors_count=approximate_neighbors_count,
                index_update_method='STREAM_UPDATE',
                **kwargs,
            )
        else:
            assert approximate_neighbors_count is None
            index = MatchingEngineIndex.create_brute_force_index(
                display_name=display_name,
                dimensions=dimensions,
                index_update_method='STREAM_UPDATE',
                **kwargs,
            )
        return cls(index.resource_name)

    def upsert_datapoints(self, datapoints: Sequence[IndexDatapoint], **kwargs) -> Self:
        """
        This method is allowed only if the `index_update_method` of the current index
        is 'STREAM_UPDATE'.
        """
        self.index.upsert_datapoints(datapoints, **kwargs)
        return self


class BatchIndex(Index):
    @classmethod
    def new(
        cls,
        display_name: str,
        *,
        dimensions: int,
        brute_force: bool = False,
        approximate_neighbors_count: int | None = None,
        **kwargs,
    ) -> Index:
        if not brute_force:
            assert approximate_neighbors_count
            index = MatchingEngineIndex.create_tree_ah_index(
                display_name=display_name,
                dimensions=dimensions,
                approximate_neighbors_count=approximate_neighbors_count,
                index_update_method='BATCH_UPDATE',
                **kwargs,
            )
        else:
            assert approximate_neighbors_count is None
            index = MatchingEngineIndex.create_brute_force_index(
                display_name=display_name,
                dimensions=dimensions,
                index_update_method='BATCH_UPDATE',
                **kwargs,
            )
        return cls(index.resource_name)

    def batch_update_datapoints(
        self,
        datapoints: Sequence[IndexDatapoint],
        *,
        staging_folder: str,
        is_complete_overwrite: bool = None,
    ) -> Self:
        """
        This method is allowed only if the `index_update_method` of the current index
        is 'BATCH_UPDATE'.

        Parameters
        ----------
        staging_folder
            A string like "gs://mybucket/.../folder".

            The `datapoints` will be written to a file called "data.json" under a new folder
            below this folder. The new, intermediate folder is named with a timestamp plus possibly
            some randomness. The intermediate folder will not conflict with any existing folders.
            This intermediate folder is needed because the operation will take all files under a folder.


            The data will be written to this file, then bulk uploaded into the index.
            If the file exists, it will be deleted first.

            The blob should not be directly under the bucket root, i.e. there should be one or more
            levels of "directory" between the bucket name and the blob.

            The file will be deleted after successful use.

        Returns
        -------
            The path of the data file (blob) that was created.
        """
        assert staging_folder.startswith('gs://')
        folder = f'{utcnow().strftime("%Y%m%d-%H%M%S")}-{str(uuid4()).split("-")[0]}'
        file = GcsBlobUpath(staging_folder, folder, 'data.json')
        file.write_bytes(
            NewlineDelimitedOrjsonSeriealizer.serialize(
                [make_batch_update_record(dp) for dp in datapoints]
            )
        )
        self.batch_update_from_uri(
            str(file.parent),
            is_complete_overwrite=is_complete_overwrite,
        )
        file.parent.rmrf()
        return self

    def batch_update_from_uri(
        self, uri: str, *, is_complete_overwrite: bool = None, **kwargs
    ) -> Self:
        """
        This method is allowed only if the `index_update_method` of the current index
        is 'BATCH_UPDATE'.

        For function details, see `MatchingEngineIndex.update_embeddings`.
        The expected structure and format of the files this URI points to is
        described at
          https://cloud.google.com/vertex-ai/docs/vector-search/setup/format-structure
        """
        assert uri.startswith('gs://')
        self.index.update_embeddings(
            contents_delta_uri=uri,
            is_complete_overwrite=is_complete_overwrite,
            **kwargs,
        )
        return self


class Endpoint:
    @classmethod
    def new(
        cls,
        display_name: str,
        *,
        description: str | None = None,
        public_endpoint_enabled: bool = False,
        labels: dict[str, str] | None = None,
        **kwargs,
    ):
        endpoint = MatchingEngineIndexEndpoint.create(
            display_name=display_name,
            public_endpoint_enabled=public_endpoint_enabled,
            description=description,
            labels=labels,
            **kwargs,
        )
        return cls(endpoint.resource_name)

    def __init__(self, name: str):
        """
        `name` is either a fully qualified "resource_name"
        (like 'projects/my-project-number/locations/us-west1/indexEndpoints/1234567890')
        or ID (like '1234567890', shown on GCP dashboard).

        If it's ID, then `init_global_config` must have been called with the correct `region` value.
        """
        self.name = name
        self._display_name = None
        self._resource_name = None

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.name}')"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.name,)

    @property
    def endpoint(self) -> MatchingEngineIndexEndpoint:
        endpoint = MatchingEngineIndexEndpoint(self.name)
        self._display_name = endpoint.display_name
        self._resource_name = endpoint.resource_name
        return endpoint

    @property
    def display_name(self) -> str:
        if self._display_name is None:
            self.endpoint
        return self._display_name

    @property
    def resource_name(self) -> str:
        if self._resource_name is None:
            self.endpoint
        return self._resource_name

    def deploy_index(
        self,
        index: Index,
        *,
        action_if_exists: Literal['return', 'replace', 'repeat', 'raise'] = 'raise',
        deployed_index_id: str | None = None,
        display_name: str | None = None,
        machine_type: str | None = None,
        min_replica_count: int = 1,
        max_replica_count: int | None = None,
        **kwargs,
    ) -> DeployedIndex:
        """
        Parameters
        ----------
        action_if_exists
            The action to take when a deployment of the index already exists on this endpoint:

            - 'return': return the existing deployment
            - 'replace': undeploy the existing one, then deploy a new one
            - 'repeat': leave the existing one intact; deploy a new (additional) one
            - 'raise': raise exception
        deployed_index_id
            Up to 128 characters long and must start with a letter and only contain
            letters, numbers, and underscores.

            Must be unique within the project it is created in.

            If missing, a value containing timestamp and some randomness will be used.
        display_name
            By default, `index.display_name` plus timestamp will be used.
        machine_type
            If missing, model is deployed with automatic resources.
        max_replica_count
            If missing, `min_replica_count * 2` is used.
        """
        if action_if_exists != 'repeat':
            for zz in self.deployed_indexes:
                if zz['index_name'] == index.resource_name:
                    if action_if_exists == 'return':
                        return self.deployed_index(zz['deployed_index_id'])
                    if action_if_exists == 'replace':
                        self.undeploy_index(zz['deployed_index_id'])
                    if action_if_exists == 'raise':
                        raise RuntimeError(
                            f"the index '{index}' is already deployed to the endpoint '{self}'"
                        )

        if not deployed_index_id:
            deployed_index_id = f'deployed_{utcnow().strftime("%Y%m%d_%H%M%S")}_{str(uuid4()).split("-")[0]}'
        if not display_name:
            display_name = (
                f'{index.display_name} since {utcnow().strftime("%Y%m%d-%H%M%S")}'
            )
        if not max_replica_count:
            max_replica_count = min_replica_count * 2
        self.endpoint.deploy_index(
            index.index,
            deployed_index_id=deployed_index_id,
            display_name=display_name,
            machine_type=machine_type,
            min_replica_count=min_replica_count,
            max_replica_count=max_replica_count,
            **kwargs,
        )
        return self.deployed_index(deployed_index_id)

    def deployed_index(self, deployed_index_id: str) -> DeployedIndex:
        return DeployedIndex(self.name, deployed_index_id)

    def undeploy_index(self, deployed_index_id: str) -> None:
        self.endpoint.undeploy_index(deployed_index_id)

    def undeploy_all(self) -> None:
        self.endpoint.undeploy_all()

    @property
    def deployed_indexes(self) -> list[dict]:
        return [
            {
                'index_name': z.index,  # resource_name
                'deployed_index_id': z.id,
                'display_name': z.display_name,
            }
            for z in self.endpoint.deployed_indexes
        ]

    def delete(self, force: bool = False) -> None:
        """
        Delete this endpoint.

        If `force` is `True`, all indexes deployed on this endpoint will be undeployed first.
        """
        self.endpoint.delete(force=force)


class DeployedIndex:
    """
    The class `DeployedIndex` is responsible for making queries to an `Index` that has been deployed at an Endpoint.
    Instantiate a `DeployedIndex` object via `Endpoint.deploy_index` or `Endpoint.deployed_index` or `Index.deploy`.
    """

    def __init__(self, endpoint_name: str, deployed_index_id: str):
        """
        `endpoint_name` is either the fully qualified "resource name"
        or the ID (if `init_global_config` has been called with the correct `region` value).
        """
        self.endpoint_name = endpoint_name
        self.deployed_index_id = deployed_index_id

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.endpoint_name}', '{self.deployed_index_id}')"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.endpoint_name, self.deployed_index_id)

    @functools.cached_property
    def endpoint(self) -> Endpoint:
        return Endpoint(self.endpoint_name)

    @functools.cached_property
    def index(self) -> Index:
        for zz in self.endpoint.deployed_indexes:
            if zz['deployed_index_id'] == self.deployed_index_id:
                return Index(zz['index_name'])
        raise RuntimeError(
            f"cannot find deployed_index_id '{self.deployed_index_id}' at endpoint {self.endpoint}"
        )

    def undeploy(self):
        self.endpoint.undeploy_index(self.deployed_index_id)

    def find_neighbors(
        self,
        queries_or_ids: Sequence[Sequence[float]] | Sequence[str],
        num_neighbors: int,
        *,
        filter: list[Namespace | Sequence | dict] | None = None,
        numeric_filter: list[NumericNamespace | Sequence | dict] | None = None,
        return_full_datapoint: bool = False,
        **kwargs,
    ) -> list[list[dict]]:
        if return_full_datapoint:
            raise NotImplementedError
            # The response conversion code is not yet implemented for this case.
        if isinstance(queries_or_ids[0], str):
            ids = queries_or_ids
            queries = None
        else:
            queries = queries_or_ids
            ids = None
        if filter:
            filter = [
                f
                if isinstance(f, Namespace)
                else (Namespace(**f) if isinstance(f, dict) else Namespace(*f))
                for f in filter
            ]
        if numeric_filter:
            numeric_filter = [
                f
                if isinstance(f, NumericNamespace)
                else (
                    NumericNamespace(**f)
                    if isinstance(f, dict)
                    else NumericNamespace(*f)
                )
                for f in numeric_filter
            ]
        resp = self.endpoint.endpoint.find_neighbors(
            deployed_index_id=self.deployed_index_id,
            queries=queries,
            embedding_ids=ids,
            num_neighbors=num_neighbors,
            filter=filter,
            numeric_filter=numeric_filter,
            return_full_datapoint=return_full_datapoint,
            **kwargs,
        )
        results = []
        for neighbors in resp:
            results.append(
                [
                    {
                        'datapoint_id': neighbor.id,
                        'dense_score': neighbor.distance if neighbor.distance else 0.0,
                    }
                    for neighbor in neighbors
                ]
            )
        assert len(results) == len(queries_or_ids)
        return results

    def get_datapoints(self, ids: Sequence[str]) -> list[IndexDatapoint]:
        """
        Retrieve datapoints with the specified IDs.
        """
        return self.endpoint.endpoint.read_index_datapoints(
            deployed_index_id=self.deployed_index_id, ids=ids
        )

    def find_datapoints(
        self,
        *,
        filter: list[Namespace | Sequence | dict] | None = None,
        numeric_filter: list[NumericNamespace | Sequence | dict] | None = None,
        max_datapoints: int = 10_000,
        **kwargs,
    ) -> list[str]:
        """
        Retrieve datapoints that satisfy the filter conditions (rather than via "close neighbor" relationship).
        """
        assert filter or numeric_filter
        embedding = [[0.0] * self.index.dimensions]
        neighbors = self.find_neighbors(
            embedding,
            num_neighbors=max_datapoints,
            filter=filter,
            numeric_filter=numeric_filter,
            **kwargs,
        )
        if neighbors:
            return [n['datapoint_id'] for n in neighbors[0]]
        return []
