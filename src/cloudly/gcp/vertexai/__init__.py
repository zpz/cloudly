from google.cloud import aiplatform

from cloudly.gcp.auth import get_project_id


def init_global_config(region: str):
    """
    Call this function once before using other utilities of this module.
    The `project` and `region` used in this call will be used in other
    functions of this module where applicable.

    `region` is like 'us-west1'.
    """
    aiplatform.init(project=get_project_id(), location=region)
