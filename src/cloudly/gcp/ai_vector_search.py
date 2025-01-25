from __future__ import annotations

__all__ = [
    'init_global_config',
    'make_datapoint',
    'make_batch_update_record',
    'Index',
    'Endpoint',
    'DeployedIndex',
]

import warnings
from collections.abc import Sequence
from typing import Any, Literal
from uuid import uuid4

from google.cloud import aiplatform
from google.cloud.aiplatform.matching_engine import (
    MatchingEngineIndex,
    MatchingEngineIndexEndpoint,
)
from typing_extensions import Self

from cloudly.upathlib.serializer import NewlineDelimitedOrjsonSeriealizer
from cloudly.util.datetime import utcnow

from .auth import get_project_id
from .storage import GcsBlobUpath

IndexDatapoint = aiplatform.compat.types.matching_engine_index.IndexDatapoint


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
        ignored_fields = set()
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
                ignored_fields.add(namespace)
        if ignored_fields:
            warnings.warn(
                f"some values in fields '{ignored_fields}' are not usable and skipped"
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

    def __init__(self, name: str):
        """
        `name` is like 'projects/166..../locations/us-west1/indexes/1234567890
        """
        self.index = MatchingEngineIndex(name)
        self.name = name

    def upsert_datapoints(self, datapoints: Sequence[IndexDatapoint], **kwargs) -> Self:
        """
        This method is allowed only if the `index_update_method` of the current index
        is 'STREAM_UPDATE'.
        """
        self.index.upsert_datapoints(datapoints, **kwargs)
        return self

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
        print(1)
        assert staging_folder.startswith('gs://')
        folder = f"{utcnow().strftime('%Y%m%d-%H%M%S')}-{str(uuid4()).split('-')[0]}"
        file = GcsBlobUpath(staging_folder, folder, 'data.json')
        print(2)
        file.write_bytes(
            NewlineDelimitedOrjsonSeriealizer.serialize(
                [make_batch_update_record(dp) for dp in datapoints]
            )
        )
        print(3)
        self.batch_update_from_uri(
            str(file.parent),
            is_complete_overwrite=is_complete_overwrite,
        )
        print(4)
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
        print('a')
        self.index.update_embeddings(
            contents_delta_uri=uri,
            is_complete_overwrite=is_complete_overwrite,
            **kwargs,
        )
        print('b')
        return self

    def remove_datapoints(self, datapoint_ids: Sequence[str]) -> Self:
        self.index.remove_datapoints(datapoint_ids)
        return self


class Endpoint:
    @classmethod
    def new(
        cls,
        display_name: str,
        *,
        public_endpoint_enabled: bool = False,
        labels: dict[str, str] | None = None,
        **kwargs,
    ):
        endpoint = MatchingEngineIndexEndpoint.create(
            display_name=display_name,
            public_endpoint_enabled=public_endpoint_enabled,
            labels=labels,
            **kwargs,
        )
        return cls(endpoint.resource_name)

    def __init__(self, name: str):
        self.name = name
        self.endpoint = MatchingEngineIndexEndpoint(name)

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
            deployed_index_id = (
                f"{utcnow().strftime('%Y%m%d_%H%M%S')}_{str(uuid4()).split('-')[0]}"
            )
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

    def undeploy_index(self, deployed_index_id: str) -> None:
        self.endpoint.undeploy_index(deployed_index_id)

    def undeploy_all(self) -> None:
        self.endpoint.undeploy_all()

    @property
    def deployed_indexes(self):
        return self.endpoint.deployed_indexes

    def delete(self, force: bool = False) -> None:
        """
        Delete this endpoint.

        If `force` is `True`, all indexes deployed on this endpoint will be undeployed first.
        """
        self.endpoint.delete(force=force)

    def deployed_idx(self, deployed_index_id: str):
        return DeployedIndex(self, deployed_index_id)


class DeployedIndex:
    """
    The class `DeployedIndex` is responsible for making queries to an `Index` that has been deployed at an endpoint.
    Instantiate a `DeployedIndex` object via `Endpoint.deploy_index` or `Endpoint.deployed_index`.
    """

    def __init__(self, endpoint: Endpoint, deployed_index_id: str):
        self.endpoint = endpoint
        self.deployed_index_id = deployed_index_id

    def find_neighbors(
        self,
        *,
        deployed_index_id: str,
        embeddings: list[list[float]],
        num_neighbors: int = 10,
    ):
        pass
