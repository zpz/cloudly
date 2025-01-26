from cloudly.gcp.ai_vector_search import (
    Endpoint,
    Index,
    init_global_config,
    make_datapoint,
)

init_global_config('us-west1')


def _test_index(index):
    endpoint = Endpoint.new('stream-endpoint', public_endpoint_enabled=True)
    print(endpoint)
    try:
        deployed_index = endpoint.deploy_index(index)
        print(deployed_index)
        deployed_index.undeploy()
    finally:
        endpoint.delete()


def test_index():
    datapoints = [
        make_datapoint(
            'a1', [0.12, 0.22, 0.13, 0.52, 0.40], {'name': 'Peter', 'age': 28}
        ),
        make_datapoint(
            'a2', [0.81, 0.19, 0.23, 0.32, 0.41], {'name': 'John', 'age': 38}
        ),
        make_datapoint(
            'a3', [0.13, 0.15, 0.33, 0.28, 0.51], {'name': 'Jack', 'age': 22}
        ),
        make_datapoint(
            'a4', [0.15, 0.13, 0.43, 0.25, 0.61], {'name': 'Paul', 'age': 25}
        ),
        make_datapoint(
            'a5', [0.17, 0.32, 0.24, 0.42, 0.31], {'name': 'Vincent', 'age': 41}
        ),
        make_datapoint(
            'a6', [0.21, 0.22, 0.25, 0.32, 0.38], {'name': 'Luke', 'age': 58}
        ),
        make_datapoint(
            'a7', [0.31, 0.12, 0.27, 0.23, 0.44], {'name': 'Zuck', 'age': 78}
        ),
    ]

    stream_index = Index.new(
        'stream-index',
        dimensions=5,
        approximate_neighbors_count=3,
        index_update_method='STREAM_UPDATE',
    )
    print(stream_index)
    try:
        stream_index.upsert_datapoints(datapoints[:3])
        _test_index(stream_index)
    finally:
        stream_index.delete()

    # TODO: batch update does not work

    # uri = GcsBlobUpath('/test/gcp/aivectorsearch', bucket_name='zpz-tmp')
    # uri.rmrf()

    # index2 = Index.new(
    #     'batch-index',
    #     dimensions=5,
    #     approximate_neighbors_count=3,
    #     index_update_method='BATCH_UPDATE',
    # )
    # print('name:', index2.name)
    # index2.batch_update_datapoints(datapoints[3:], staging_folder=str(uri))
