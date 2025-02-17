import math

import pytest

from cloudly.gcp.storage import GcsBlobUpath
from cloudly.gcp.vertexai import init_global_config
from cloudly.gcp.vertexai.vector_search import (
    BatchIndex,
    Endpoint,
    Index,
    StreamIndex,
    make_datapoint,
)

init_global_config('us-west1')


DATAPOINTS = [
    make_datapoint('a1', [0.12, 0.22, 0.13, 0.52, 0.40], {'name': 'Peter', 'age': 28}),
    make_datapoint('a2', [0.81, 0.19, 0.23, 0.32, 0.41], {'name': 'John', 'age': 38}),
    make_datapoint('a3', [0.13, 0.15, 0.33, 0.28, 0.51], {'name': 'Jack', 'age': 22}),
    make_datapoint('a4', [0.15, 0.13, 0.43, 0.25, 0.61], {'name': 'Paul', 'age': 25}),
    make_datapoint(
        'a5', [0.17, 0.32, 0.24, 0.42, 0.31], {'name': 'Vincent', 'age': 41}
    ),
    make_datapoint('a6', [0.21, 0.22, 0.25, 0.32, 0.38], {'name': 'Luke', 'age': 58}),
    make_datapoint('a7', [0.31, 0.12, 0.27, 0.23, 0.44], {'name': 'Zuck', 'age': 78}),
    make_datapoint('a8', [0.31, 0.12, 0.27, 0.23, 0.44], {'name': 'John', 'age': 33}),
]


def _test_index(index: Index):
    print('index', index)
    print('    name', index.name)
    print('    display_name', index.display_name)
    print('    resource_name', index.resource_name)
    print('    dimensions', index.dimensions)

    assert index.dimensions == 5
    assert not index.deployed_indexes

    assert len(index) == len(DATAPOINTS)

    endpoint = Endpoint.new('stream-endpoint', public_endpoint_enabled=True)
    print('endpoint', endpoint)
    print('    name', endpoint.name)
    print('    resource_name', endpoint.resource_name)
    print('    display_name', endpoint.display_name)
    assert not endpoint.deployed_indexes

    di1 = endpoint.deploy_index(index)
    print('deployed_index', di1)
    print('    index', di1.index)
    print('    endpoint', di1.endpoint)
    zz = endpoint.deployed_indexes
    assert len(zz) == 1

    dp = di1.get_datapoints([d.datapoint_id for d in DATAPOINTS[:2]])
    for a, b in zip(dp, DATAPOINTS[:2]):
        assert a.datapoint_id == b.datapoint_id
        assert all(
            math.isclose(x, y) for x, y in zip(a.feature_vector, b.feature_vector)
        )

        neighbors = di1.find_neighbors(['a3', 'a4'], num_neighbors=3)
        assert len(neighbors) == 2
        assert all(len(v) == 3 for v in neighbors)
        print(neighbors)

    dp = di1.find_datapoints(filter=[('name', ['John'], [])])
    assert len(dp) == 2
    print(dp)

    di2 = index.deploy(endpoint.name, action_if_exists='repeat')
    print(di2)
    assert len(endpoint.deployed_indexes) == 2
    endpoint.undeploy_index(di2.deployed_index_id)
    assert len(index.deployed_indexes) == 1
    di1.undeploy()

    index.remove_datapoints(['a1', 'a2', 'a4'])
    assert len(index) == len(DATAPOINTS) - 3
    index.remove_datapoints(['a4', 'a5'])
    assert len(index) == len(DATAPOINTS) - 4

    endpoint.delete()


@pytest.mark.skip(reason='not ready, too slow')
def test_stream_index():
    print()
    index = StreamIndex.new(
        'stream-index',
        dimensions=5,
        approximate_neighbors_count=3,
    )
    print('name:', index.name)

    index.upsert_datapoints(DATAPOINTS[:3])
    assert len(index) == 4

    index.upsert_datapoints(DATAPOINTS[3:])
    assert len(index) == len(DATAPOINTS)

    _test_index(index)

    index.undeploy_all()
    index.delete()


@pytest.mark.skip(reason='not ready, too slow')
def test_batch_index():
    uri = GcsBlobUpath('/test/gcp/aivectorsearch', bucket_name='zpz-tmp')
    uri.rmrf()

    index = BatchIndex.new(
        'batch-index',
        dimensions=5,
        approximate_neighbors_count=3,
    )
    print('name:', index.name)

    index.batch_update_datapoints(DATAPOINTS[:4], staging_folder=str(uri))
    assert len(index) == 4

    index.batch_update_datapoints(DATAPOINTS[4:], staging_folder=str(uri))
    assert len(index) == len(DATAPOINTS)

    _test_index(index)

    index.undeploy_all()
    index.delete()
