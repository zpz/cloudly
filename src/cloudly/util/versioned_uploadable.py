import warnings

from cloudly.upathlib.versioned_uploadable import *  # noqa: F403

warnings.warn(
    '`cloudly.util.versioned_uploadable` is deprecated in 0.2.4 and will be removed in 0.3.0 or later; please use `cloudly.upathlib.versioned_uploadable` instead',
    DeprecationWarning,
    stacklevel=2,
)
