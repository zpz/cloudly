__all__ = ['get_secret']

from google.cloud import secretmanager

from .auth import get_project_id


def get_secret(name: str, *, version: str = 'latest'):
    client = secretmanager.SecretManagerServiceClient()
    secret_uri = f'projects/{get_project_id()}/secrets/{name}/versions/{version}'
    response = client.access_secret_version(request={'name': secret_uri})
    # This may raise `google.api_core.exceptions.NotFound`
    return response.payload.data.decode('utf-8')
