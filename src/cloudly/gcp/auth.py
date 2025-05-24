"""
This module uses GCP's default mechanisms to find account info.
This mechanism works if the code is running on a GCP machine.

Code that runs on a GCP machine may be able to infer ``credentials`` and ``project_id``
via `google.auth.default() <https://googleapis.dev/python/google-auth/latest/user-guide.html#application-default-credentials>`_.

If the code is running on a non-GCP machine but needs to interact with GCP,
you can set up environment variables to "impersonate" a GCP machine.
The function :func:`set_env` is one way to set this up.

See: https://google.aip.dev/auth/4110
     https://stackoverflow.com/questions/44328277/how-to-auth-to-google-cloud-using-service-account-in-python
"""

__all__ = ['set_env', 'get_project_id', 'get_credentials', 'get_service_account_email']

import json
import os
from datetime import datetime, timezone

import google.auth
import google.auth.credentials
from google.api_core.exceptions import RetryError
from google.api_core.retry import Retry, if_exception_type
from google.auth.transport.requests import Request

_PROJECT_ID = None
_CREDENTIALS = None


def set_env(
    *,
    project_id: str,
    private_key_id: str,
    private_key: str,
    client_id: str,
    client_email: str,
    path: str | None = None,
) -> None:
    """
    This function writes credentials info into a "credential file" and sets the environment variable
    `GOOGLE_APPLICATION_CREDENTIALS` to point to that file. GCP client libraries use that env var
    to find so-called "Application Default Credentials (ADC)". This setup is needed for GCP client code
    to run (i.e. to communicate with GCP services) on a non-GCP machine. If the code is running on a GCP machine
    (already in your account), this environment is already set up for you.

    This function needs to be called only once in the program.
    Env vars set by `os.environ` carries over into other processes created using the ``multiprocessing`` module.

    However, this is not the only way to set up credentials. Depending on the type of your account,
    different fields may be needed. For example, I have a personal account of the type "authorized_user". For that type,
    "project_id" is not available in the credential file. (Even if `project_id` is included in that file, it will be ignored.)
    I need to use a second env var `GOOGLE_CLOUD_PROJECT` to provide project ID.

    If appropriate env vars are already set up (as is the case on a GCP machine), there is no need to call this function.

    To get these credentials for your GCP account, look for "create access credentials" or "create credentials for a service account"
    under "google workspace".
    """
    private_key = (
        '-----BEGIN PRIVATE KEY-----\n'
        + private_key.encode('latin1').decode('unicode_escape')
        + '\n-----END PRIVATE KEY-----\n'
    )
    info = {
        'type': 'service_account',
        'project_id': project_id,
        'private_key_id': private_key_id,
        'private_key': private_key,
        'client_email': client_email,
        'client_id': client_id,
        'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
        'token_uri': 'https://oauth2.googleapis.com/token',
        'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
        'client_x509_cert_url': f'https://www.googleapis.com/robot/v1/metadata/x509/{client_email.replace("@", "%40")}',
    }
    if path:
        path = os.path.abspath(path)
    else:
        path = os.path.expanduser('~') + '/._gcp_credentials'
    with open(path, 'w') as file:
        json.dump(info, file)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path

    global _PROJECT_ID
    global _CREDENTIALS
    _PROJECT_ID = project_id
    _CREDENTIALS = None


def get_project_id() -> str:
    global _PROJECT_ID
    global _CREDENTIALS
    if _PROJECT_ID:
        return _PROJECT_ID
    _CREDENTIALS, _PROJECT_ID = google.auth.default(
        scopes=['https://www.googleapis.com/auth/cloud-platform'],
    )
    return _PROJECT_ID


def get_credentials(
    *, valid_for_seconds: int = 600, return_state: bool = False
) -> (
    google.auth.credentials.Credentials
    | tuple[google.auth.credentials.Credentials, bool]
):
    """
    `valid_for_seconds`: the credentials should be valid for at least this many seconds;
        if the existing credential would expire sooner than this, renew it.
    `return_state`: if `True`, return whether credentials have been renewed in this call;
        if `False`, do not return this info.
    """
    global _PROJECT_ID
    global _CREDENTIALS
    renewed = False
    if not _CREDENTIALS:
        _CREDENTIALS, _PROJECT_ID = google.auth.default(
            scopes=['https://www.googleapis.com/auth/cloud-platform'],
        )
        renewed = True
    credentials = _CREDENTIALS
    if (
        not credentials.token
        or (
            credentials.expiry - datetime.now(timezone.utc).replace(tzinfo=None)
        ).total_seconds()
        < valid_for_seconds
    ):
        try:
            Retry(
                predicate=if_exception_type(
                    google.auth.exceptions.RefreshError,
                    google.auth.exceptions.TransportError,
                ),
                initial=1.0,
                maximum=10.0,
                timeout=300.0,
            )(credentials.refresh)(Request())
            # This token expires in one hour.
        except RetryError as e:
            raise e.cause
        renewed = True

    if return_state:
        return credentials, renewed
    return credentials
    # This object has attributes "token", "expiry".


def get_service_account_email() -> str:
    if _CREDENTIALS:
        return _CREDENTIALS.service_account_email
    return get_credentials().service_account_email
