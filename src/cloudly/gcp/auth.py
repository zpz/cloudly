"""
This module uses GCP's default mechanisms to find account info.
This mechanism works if the code is running on a GCP machine.

Code that runs on a GCP machine may be able to infer ``credentials`` and ``project_id``
via `google.auth.default() <https://googleapis.dev/python/google-auth/latest/user-guide.html#application-default-credentials>`_.

If the code is running on a non-GCP machine but needs to interact with GCP,
you can call :func:`set_env` to provide env vars that are expected by the GCP mechanism.

See: https://google.aip.dev/auth/4110

"""

import json
import os
from datetime import datetime, timezone

import google.auth
from google.api_core.exceptions import RetryError
from google.api_core.retry import Retry, if_exception_type

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
):
    """
    This function sets up env var(s) on a non-GCP machine so that GCP's default mechanism can find account
    credentials as if the code is running on a GCP machine.

    I'm not sure all the content of ``info`` is required.

    Alternatively, if you have the dict ``info``,  the account ``credentials`` can be obtained by the following
    function call

    ::

        google.oauth2.service_account.Credentials.from_service_account_info(
            info, scopes=['https://www.googleapis.com/auth/cloud-platform'])
    """
    info = {
        'type': 'service_account',
        'project_id': project_id,
        'private_key_id': private_key_id,
        'private_key': '-----BEGIN PRIVATE KEY-----\\n'
        + private_key.encode('latin1').decode('unicode_escape')
        + '\\n-----END PRIVATE KEY-----\\n',
        'client_email': client_email,
        'client_id': client_id,
        'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
        'token_uri': 'https://oauth2.googleapis.com/token',
        'auth_provider_x509_cert_url': 'https://www.googleapis.com/oauth2/v1/certs',
        'client_x509_cert_url': f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email.replace('@', '%40')}",
    }

    if not path:
        path = os.path.expanduser('~') + '/._gcp_credentials'
    with open(path, 'w') as file:
        json.dump(info, file)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path


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
    valid_for_seconds: int = 600, *, return_state: bool = False
) -> str | tuple[str, bool]:
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
            )(credentials.refresh)(google.auth.transport.requests.Request())
            # One check shows that this token expires in one hour.
        except RetryError as e:
            raise e.cause
        renewed = True

    if return_state:
        return credentials, renewed
    return credentials
