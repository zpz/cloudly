__all__ = [
    'gcp_handler',
    'add_gcp_handler',
    'config_logger',
]


import logging
from typing import IO

from google.cloud.logging import Client
from google.cloud.logging.handlers import CloudLoggingHandler
from google.cloud.logging_v2.handlers.transports import BackgroundThreadTransport

from cloudly.util.logging import DynamicFormatter, rootlogger, set_level

from .auth import get_credentials, get_project_id

logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)


def gcp_handler(
    name: str,
    *,
    labels: dict = None,
    grace_period: float = None,
    batch_size: int = None,
    max_latency: float = None,
    stream: IO = None,
) -> logging.Handler:
    """
    `name` is the "log name" shown on GCP logging dashboard.
    A unique name enables user to inspect the particular program's log in isolation.

    User may want to pass in `labels` with info of interest to their application.
    """
    if grace_period is None:
        grace_period = 5.0  # seconds; same as GCP default
    if batch_size is None:
        batch_size = 100  # GCP default is 10
    if max_latency is None:
        max_latency = 0.1  # seconds; GCP default is 0

    class BackgroundTransport(BackgroundThreadTransport):
        def __init__(self, *args, **kwargs):
            super().__init__(
                *args,
                grace_period=grace_period,
                batch_size=batch_size,
                max_latency=max_latency,
                **kwargs,
            )

    client = Client(credentials=get_credentials(), project=get_project_id())
    handler = CloudLoggingHandler(
        client, name=name, transport=BackgroundTransport, labels=labels, stream=stream
    )

    return handler


def add_gcp_handler(
    name,
    *,
    labels=None,
    with_datetime=False,
    with_timezone=False,
    with_level=False,
    **kwargs,
) -> logging.Handler:
    h = gcp_handler(name=name, labels=labels, **kwargs)
    h.setFormatter(
        DynamicFormatter(
            with_datetime=with_datetime,
            with_timezone=with_timezone,
            with_level=with_level,
        )
    )
    rootlogger.addHandler(h)
    return h


def config_logger(name, level=logging.INFO, **kwargs):
    # Because you're expected to call `set_level` only once,
    # if you use more than one handlers, e.g. to the cloud and to the console,
    # then you should not call this convenience function.
    # Instead, add the handlers to `rootlogger` and call `set_level` once.
    # Example::
    #
    #   add_console_handler(...)
    #   add_gcp_handler(...)
    #   set_level(...)

    set_level(level)
    add_gcp_handler(name, **kwargs)
