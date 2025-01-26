from __future__ import annotations

__all__ = [
    'init_global_config',
    'make_datapoint',
    'make_batch_update_record',
    'Index',
    'Endpoint',
    'DeployedIndex',
    'IndexDatapoint',
]

from collections.abc import Sequence
from typing import Any, Literal
from uuid import uuid4

from google.cloud import aiplatform
from google.cloud.aiplatform import matching_engine
from typing_extensions import Self

from cloudly.upathlib.serializer import NewlineDelimitedOrjsonSeriealizer
from cloudly.util.datetime import utcnow

from .auth import get_project_id
from .storage import GcsBlobUpath

IndexDatapoint = aiplatform.compat.types.matching_engine_index.IndexDatapoint
Namespace = matching_engine.matching_engine_index_endpoint.Namespace
NumericNamespace = matching_engine.matching_engine_index_endpoint.NumericNamespace
MatchingEngineIndex = matching_engine.MatchingEngineIndex
MatchingEngineIndexEndpoint = matching_engine.MatchingEngineIndexEndpoint


def init_global_config(region: str):
    """
    Call this function once before using :class:`Index` and :class:`Endpoint`.
    The `project` and `region` used in this call will be used in other
    functions of this module where applicable.

    `region` is like 'us-west1'.
    """
    aiplatform.init(project=get_project_id(), location=region)


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
    """

    @classmethod
    def new(
        cls,
        display_name: str,
        *,
        dimensions: int,
        brute_force: bool = False,
        approximate_neighbors_count: int | None = None,
        index_update_method: Literal['STREAM_UPDATE', 'BATCH_UPDATE'] = 'BATCH_UPDATE',
        **kwargs,
    ) -> Index:
        if not brute_force:
            assert approximate_neighbors_count
            index = MatchingEngineIndex.create_tree_ah_index(
                display_name=display_name,
                dimensions=dimensions,
                approximate_neighbors_count=approximate_neighbors_count,
                index_update_method=index_update_method,
                **kwargs,
            )
        else:
            assert approximate_neighbors_count is None
            index = MatchingEngineIndex.create_brute_force_index(
                display_name=display_name,
                dimensions=dimensions,
                index_update_method=index_update_method,
                **kwargs,
            )
        return cls(index.resource_name)

    def __init__(self, resource_name: str):
        """
        `resource_name` is a fully qualified index resource name,
        like 'projects/166..../locations/us-west1/indexes/1234567890
        """
        self.index = MatchingEngineIndex(resource_name)
        self.resource_name = resource_name
        self.display_name = self.index.display_name

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.resource_name}')"

    def __str__(self):
        return self.__repr__()

    def upsert_datapoints(self, datapoints: Sequence[IndexDatapoint], **kwargs) -> Self:
        """
        This method is allowed only if the `index_update_method` of the current index
        is 'STREAM_UPDATE'.
        """
        self.index.upsert_datapoints(datapoints, **kwargs)
        return self

    # TODO: this is not working
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
        folder = f"{utcnow().strftime('%Y%m%d-%H%M%S')}-{str(uuid4()).split('-')[0]}"
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

    # TODO: this is not working
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
        print('uri', uri)
        self.index.update_embeddings(
            contents_delta_uri=uri,
            is_complete_overwrite=is_complete_overwrite,
            **kwargs,
        )
        return self

    def remove_datapoints(self, datapoint_ids: Sequence[str]) -> Self:
        self.index.remove_datapoints(datapoint_ids)
        return self

    @property
    def deployed_indexes(self):
        raise NotImplementedError

    def delete(self):
        """
        Delete this index (permanently).
        """
        self.index.delete()


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

    def __init__(self, resource_name: str):
        self.resource_name = resource_name
        self.endpoint = MatchingEngineIndexEndpoint(resource_name)
        self.display_name = self.endpoint.display_name

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.resource_name}')"

    def __str__(self):
        return self.__repr__()

    def deploy_index(
        self,
        index: Index,
        *,
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
        if not deployed_index_id:
            deployed_index_id = f"deployed_{utcnow().strftime('%Y%m%d_%H%M%S')}_{str(uuid4()).split('-')[0]}"
        if not display_name:
            display_name = (
                f"{index.display_name} since {utcnow().strftime('%Y%m%d-%H%M%S')}"
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
        return self.deployed_idx(deployed_index_id)

    def deployed_idx(self, deployed_index_id: str):
        return DeployedIndex(self, deployed_index_id)

    def undeploy_index(self, deployed_index_id: str) -> None:
        self.endpoint.undeploy_index(deployed_index_id)

    def undeploy_all(self) -> None:
        self.endpoint.undeploy_all()

    @property
    def deployed_indexes(self) -> list[tuple[str, str]]:
        # Return 'deployed_index_id' and 'index_resource_name' tuples.
        zz = self.endpoint.deployed_indexes
        zz = sorted(((d.id, d.index) for d in zz))
        return zz

    def delete(self, force: bool = False) -> None:
        """
        Delete this endpoint.

        If `force` is `True`, all indexes deployed on this endpoint will be undeployed first.
        """
        self.endpoint.delete(force=force)

    # TODO:
    # checkout `Index.index.to_dict()['metadata']`


class DeployedIndex:
    """
    The class `DeployedIndex` is responsible for making queries to an `Index` that has been deployed at an endpoint.
    Instantiate a `DeployedIndex` object via `Endpoint.deploy_index` or `Endpoint.deployed_index`.
    """

    def __init__(self, endpoint: Endpoint, deployed_index_id: str):
        self.endpoint = endpoint
        self.deployed_index_id = deployed_index_id
        self._index_resource_name = None

    def __repr__(self):
        return f"{self.__class__.__name__}({self.endpoint}, '{self.deployed_index_id}')"

    def __str__(self):
        return self.__repr__()

    @property
    def index_resource_name(self) -> str:
        if not self._index_resource_name:
            zz = self.endpoint.endpoint.deployed_indexes
            for z in zz:
                if z.id == self.deployed_index_id:
                    self._index_resource_name = z.index
                    break
            if not self._index_resource_name:
                raise Exception('could not find the deployed index on the endpoint')
        return self._index_resource_name

    @property
    def index(self) -> Index:
        return Index(self.index_resource_name)

    def undeploy(self):
        self.endpoint.undeploy_index(self.deployed_index_id)

    def find_neighbors(
        self,
        queries_or_embedding_ids: Sequence[Sequence[float]] | Sequence[str],
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
        if isinstance(queries_or_embedding_ids[0], str):
            embedding_ids = queries_or_embedding_ids
            queries = None
        else:
            queries = queries_or_embedding_ids
            embedding_ids = None
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
        resp = self.endpoint.find_neighbors(
            deployed__index_id=self.deployed_index_id,
            queries=queries,
            embedding_ids=embedding_ids,
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
                        'embedding_id': neighbor.id,
                        'dense_score': neighbor.distance if neighbor.distance else 0.0,
                    }
                    for neighbor in neighbors
                ]
            )
        assert len(results) == len(queries_or_embedding_ids)
        return results

    def get_datapoints_by_ids(self):
        raise NotImplementedError

    def get_datapoints_by_filters(self):
        raise NotImplementedError
