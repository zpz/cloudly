from cloudly.gcp.secretmanager import get_secret


def test_get_secret():
    assert '4' == get_secret('SQUARE_OF_TWO')
