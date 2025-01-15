from __future__ import annotations

import warnings
from collections.abc import Sequence
from typing import Any
from uuid import uuid4

from google.cloud import aiplatform
from google.cloud.aiplatform.compat.types.matching_engine_index import IndexDataPoint
from google.cloud.aiplatform.matching_engine import (
    MatchingEngineIndex,
)

from cloudly.upathlib.serializer import NewlineDelimitedOrjsonSeriealizer
from cloudly.util.datetime import utcnow

from .auth import get_project_id
from .storage import GcsBlobUpath


def init_global_config(region: str = 'us-central-1'):
    aiplatform.init(project=get_project_id(), location=region)


def make_datapoint(
    id_: str, embedding: list[float], properties: dict[str, Any] | None = None
) -> IndexDataPoint:
    # TODO: look for a more suitable name for `properties`.
    restricts = []
    numeric_restricts = []
    if properties:
        ignored_fields = set()
        for namespace, value in properties.items():
            assert isinstance(namespace, str)
            if isinstance(value, str):
                restricts.append(
                    IndexDataPoint.Restriction(namespace=namespace, allow_list=[value])
                )
            elif isinstance(value, list) and all(isinstance(v, str) for v in value):
                restricts.append(
                    IndexDataPoint.Restriction(namespace=namespace, allow_list=value)
                )
            elif isinstance(value, (int, float)):
                numeric_restricts.append(
                    IndexDataPoint.NumericRestriction(
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
    return IndexDataPoint(
        datapoint_id=id_,
        feature_vector=embedding,
        restricts=restricts,
        numeric_restricts=numeric_restricts,
    )


def make_batch_update_record(datapoint: IndexDataPoint) -> dict:
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
        **kwargs,
    ) -> Index:
        if not brute_force:
            assert approximate_neighbors_count
            index = MatchingEngineIndex.create_tree_ah_index(
                display_name=display_name,
                dimensions=dimensions,
                approximate_neighbors_count=approximate_neighbors_count,
                **kwargs,
            )
        else:
            assert approximate_neighbors_count is None
            index = MatchingEngineIndex.create_brute_force_index(
                display_name=display_name,
                dimensions=dimensions,
                **kwargs,
            )
        return cls(index.resource_name)

    def __init__(self, index_name: str):
        self._index = MatchingEngineIndex(index_name)

    @property
    def index(self) -> MatchingEngineIndex:
        return self._index

    def upsert_datapoints(self, datapoints: Sequence[IndexDataPoint], **kwargs):
        # "stream updating"
        self.index.upsert_datapoints(datapoints, **kwargs)

    def batch_update_datapoints(
        self,
        datapoints: Sequence[IndexDataPoint],
        *,
        staging_folder: str,
        is_complete_overwrite: bool = None,
    ) -> str:
        """
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

            The file will not be deleted after use.

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
        self.index.update_embeddings(
            contents_delta_uri=str(file.parent),
            is_complete_overwrite=is_complete_overwrite,
        )
        return str(file)

    def batch_update_from_uri(
        self, uri: str, *, is_complete_overwrite: bool = None, **kwargs
    ):
        # For function details, see `MatchingEngineIndex.update_embeddings`.
        # The expected structure and format of the files this URI points to is
        # described at
        #   https://cloud.google.com/vertex-ai/docs/vector-search/setup/format-structure
        assert uri.startswith('gs://')
        self.index.update_embeddings(
            contents_delta_uri=uri,
            is_complete_overwrite=is_complete_overwrite,
            **kwargs,
        )

    def remove_datapoints(self, datapoint_ids: Sequence[str]):
        self.index.remove_datapoints(datapoint_ids)


class Endpoint:
    """
    The class `Endpoint` is responsible for making queries to the `Index` (which has been
    "deployed" at the endpoint).
    """

    def find_neighbors(self):
        pass
