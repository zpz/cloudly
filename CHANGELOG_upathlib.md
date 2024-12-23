# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


## [0.9.8] - 2024-11-18

- `Multiplexer` object now supports pickling.
- Function `get_google_auth` was removed; now using utilities from `cloudly`.


## [0.9.7] - 2024-09-02

- API changes to the methods `copy_file` and `copy_dir`: the first parameter used to be the "target", now it is the "source".
  Now, both methods write to `self`. This is consistent with the `write_*` methods of files.
- Clarified how `download_*` and `upload_*` implementations of a blob store relate to the generic `copy_dir` and `copy_file`
  methods. Now, calling `copy_dir` and `copy_file` will delegate to tailored `download_*` and `upload_*` methods when available.


## [0.9.6] - 2024-06-20

- Finetuning about GCS retry.


## [0.9.5] - 2024-06-07

- `GcsBlobUpath.write_meta` adds retry on `ServiceUnavailable`.


## [0.9.4] - 2024-02-07

- Finetune `Multiplexer`: removing a data element from internal data list before yielding it.


## [0.9.3] - 2024-01-31

- `GcsBlobUpath` gets new methods `read_meta` and `write_meta`.
- Revise the design of `GcsBlobUpath.lock` so that user does not need to lock a "helper file"; instead,
  user locks the file they want to read/write, and this method internally locks a helper file.
- `Upath.lock` now yields `self` in the context manager.
- Removed `upathlib.gcs` in favor of `upathlib._gcs`.
- Some finetuning to GCS internals.


## [0.9.2] - 2024-01-25

- Refinements to `upathlib.serializer`:
  - Removed `TextSerializer`.
  - The base is the new protocol `Serializer`.
  - Besides `serialize` and `deserialize`, it gets two new classmethods: `dump` and `load`.
  - Added `orjson` serializers.
- Removed methods `Upath.write_pickle_z`, `Upath.read_pickle_z`, `Upath.write_pickle_lz4`, `Upath.read_pickle_lz4`.
- Bumped Python versoin to 3.10, although there's no firm feature requirement that I know of at this time.


## [0.9.1] - 2024-01-22

- Lower log level from 'info' to 'debug' for one message.


## [0.9.0] - 2024-01-18

- Bug fix in GCS related to retry and locking.
- Changed `upathlib.azure` to `upathlib._azure`.
- Changed `upathlib.gcs` to `upathlib._gcs`; added `upathlib._gcs` for back-compat transition.
- Made `get_google_auth` importable from `upathlib.__init__`.
- Changed `upathlib._multiplexer` to `upathlib.multiplexer`; removed `Multiplexer` from `upathlib.__init__`.
- Improvements to error messages.


## [0.8.9] - 2024-01-14

- New class ``Multiplexer``.
- Finetune in GCS ratelimit handling in ``write_bytes``.


## [0.8.8] - 2023-10-26

- Fixes and finetuning on GCS "retry" logic.


## [0.8.7] - 2023-10-19

- Removed optional depencency ``lz4``. User can install it if they need to use it.
- Fix: the parameter `timeout` for `GcsBlobUpath.lock` should be applied to the `release_retry`
  in addition to `acquire_retry`.


## [0.8.6] - 2023-09-30
- Make ``ZstdPickleSerializer`` thread safe.
- Remove functions ``z_compress``, ``z_decompress``, ``zstd_compress``, ``zstd_decompress``, ``lz4_compress``, ``lz4_decompress``.


## [0.8.5] - 2023-09-27

- `zstandard` became a required dependency.
- Enhancements to `ZstdPickleSerializer`.


## [0.8.4] - 2023-09-13

- Finetune "delay time" in the retry logic of GCS locking.


## [0.8.3] - 2023-08-15

- Add retries on GCS write rate-limiting error.
- Finetune retries in GCS locking.


## [0.8.2] - 2023-08-06

- Add certain retries on Google authentication.
- Adjust retry delays in GCS locking.


## [0.8.1] - 2023-06-18

- ``google_api_core.Retry.timeout`` workaround
- Remove dependency on ``mpservice``; the new module ``_util.py`` is copied from ``mpservice``.


## [0.8.0] - 2024-04-25

### Fixed

- Bug in GCS locking related to retry condition.


## [0.7.9] - 2023-04-24

- Upgrade `mpservice``.


## [0.7.8] - 2023-04-19

### Changed

- ``LocalUpath.lock`` no longer deletes the file after lock release.

### Enhanced

- Fine-tuned "retry" logic in GCS.
- Fine-tuned ``LocalUpath.lock``.
- Support parameter ``concurrent: bool`` in folder operations (and GCS file download) where possible.


## [0.7.7] - 2023-04-14

### Added

- Methods that use thread pool get parameter `concurrent` with default `True`.


## [0.7.6] - 2023-04-10

### Removed

- `orjson` related methods.
- all uses of `overrides`

### Added

- optional dependency `lz4`
- `Lz4PickleSerializer`
- functions `z_compress`, `z_decompress`, `zstd_compress`, `zstd_decompress`, `lz4_compress`, `lz4_decompress`
- methods `write_pickle_lz4`, `read_pickle_lz4`


### Changed

- `zstandard` becomes an "optional", rather than mandatory, dependency.


## [0.7.5] - 2023-03-31

### Fixed

- Fine-tune multipart download of large blobs from Google Cloud Storage.


## [0.7.4] - 2023-03-21

Upgraded Python to 3.10 for development and testing.
Fixed an error in parameter type annotation related to `overrides`
that was revealed in this migration.
There was no changes to any functionality.


## [0.7.3] - 2023-03-19

### Deprecated or removed

- Deprecated orjson serializers and read/write methods.

### Added

- `LocalUpath.write_bytes` now accepts file-like data as input.


## [0.7.2] - 2023-03-06

### Bug fixes

- Suppress progress printouts in `rmrf`.
- Bug fix and clarification on `lock`---upon exiting the context manager, the lock file must be deleted.


## [0.7.1] - 2023-02-25

- Fine-tune `GcsBlobUpath.lock`.
- Run doctest in `test_docs.py`.
- Fix dependency `filelock` version and hack it to use `time.perf_counter` instead of `time.monotonic`.
- Use `mpservice` for global thread pools to make them safe with forked processes.


## [0.7.0] - 2023-01-20

- `GcsBlobUpath` finetune and bug fix related to exceptions, retry, lock.
- `LocalUpath.lock` finetune.


## [0.6.9] - 2023-01-07

### Removed

- Parameters `project_id` and `credentials` to `GcsBlobUpath.__init__`.
- Classmethods `register_read_write_byte_format`, `register_read_write_text_format` of `Upath`.
- Parameter `thread_pool_executors` to `Upath.__init__`.
- Methods `rename_dir` and `rename_file` in `Upath` (both remain in `LocalUpath`).
- Classes `ZJsonSerializer`, `ZstdJsonSerializer`.
- Back-compat module `upathlib.gcp`.
- Methods `export_dir`, `export_file`, `import_dir`, `import_file`. (Concentrate on the `copy_*` methods.)
- Method `with_path` renamed to `_with_path` and has become an intermediate implementation helper based on the new property `root`.
- Properties `GcsBlobUpath.{client, bucket}` have become private methods `_client`, `_bucket`.
- Method `GcsBlobUpath.blob` has become private `_blob`.
- `GcsBlobUpath.get_blob` is removed.

### Deprecated

- `LocalUpath.localpath`. (Use `LocalUpath.path` instead.)

### Changed

- `LocalUpath.path` overrides the super version to return `pathlib.Path`.
- The tests module `upathlib.tests` was renamed `_tests`.
- Simplified comparison and ordering special methods.

### Added or enhanced

- Enhancements to documentation, including Sphinx documentation generation and hosting on readthedocs.
- New method `as_uri`. Comparison, ordering, and hash special methods are changed to use the output of `as_uri`.
- `LocalUpath` now implements the `os.PathLike` protocol.
- Methods `read_text`, `write_text`, `read_json`, `write_json` get parameters `encoding` and `errors`.
- New property `root`.
- Initial support for Windows by `LocalUpath`. `test_local.py` passed on Windows, but there could be corner cases that will fail on Windows.


## [0.6.8] - 2022-11-16

- New helper function `resolve_path`.
- Many directory operations dropped the `desc` parameter but gained the `quiet` parameter, defaulting to `False`. But, `rmdir` and `rmrf` default to `quiet=True`.
- `GcpBlobUpath` is also exposed in `__init__.py`.
- Parameters `project_id` and `credentials` of `GcpBlobUpath` are deprecated. This info is moved to a classmethod. Similar changes to `AzureBlobUpath`.
- Renamed `upathlib.gcp` to `upathlib.gcs`, and `GcpBlobUpath` to `GcsBlobUpath`.


## [0.6.7] - 2022-11-03

- Handle "empty folders" in GCP.
- Use Google's standard way (via `google.auth.default`) to get GCP credentials if needed.
- Allow disabling progress bar in most cases by setting `desc` to `False`.
- Thread-pool management; `_run_in_executor` became an instance method (as opposed to classmethod).


## [0.6.6] - 2022-10-07

- Improvement to robustness in large directory upload to Gcp.
- Improved progress report when downloading/uploading a directory.
- Fine-tuned methods `import_file` and `export_file`.
- Bug fix in `GcpBlotUpath.with_path`. The bug causes scalability (when operating on upwards of 56000 files) and speed issues in `upload_dir`, because every blob will create its own `Client`.
- Removed `LockAcquisitionTimeoutError`. Added `LockAcquireError`, `LockReleaseError`.
- `GcpBlobUpath.lock` reduces default wait time to improve responsiveness.


## [0.6.5] - 2022-07-30

- Make GCP info to `GcpBlobUpath` optional.
- Remove home-made retrying utility `Backoff`. Use `opnieumw`.
- Bug fix related to concurrent downloading involving large files.


## [0.6.4] - 2022-07-10

- GCP download of large blobs uses threading concurrency.
- API change: `write_bytes` and `write_text` return `None`.
- Improvements to handling of concurrency.
- Increased default concurrency level from 4 to 16.
- Simplified retry logic. For example, GCP's `download_to_file` has its own
  handling of retry; now we rely on `download_to_file` to finish the task
  and do not retry on it.
- Simplified parameters and behavior around 'overwrite'.
- Improvements to serializers:
  - Serializers allow extra arguments in their `serialize` and `deserialize` methods.
  - Added dependency `zstandard` to provide compression.
  - New serializers `ZJsonSerializer`, `ZstdJsonSerializer`, `ZstdPickleSerializer`, `ZstdOrjsonSerializer`.


## [0.6.3] - 2022-03-06

- Use `overrides` to enforce sanity in class inheritance.
- Add tests on Azure and GCP using mocks.
- Add more retry logic to GCP.
- Refactor and simplify test/build process.


## [0.6.2] - 2022-02-22

- GcpUpath: refresh cache upon timeout error.


## [0.6.1.post1] - 2021-12-31

- Relax version requirements on dependencies.


## [0.6.1] - 2021-11-11

- Minor fine-tuning.


## [0.6.0.post1] - 2021-11-05

- Bug fix.


## [0.6.0] - 2021-11-04

- Bug fix in GcpBlobUpath


## [0.5.9] - 2021-10-15

- Removed async methods, since the current simplementations are simply wrappers around thread-pool executions.


## [0.5.8] - 2021-10-15

- Refactor the `__init__` method.
- Efficiency improvements for GCP.


## [0.5.7] - 2021-10-10

- Capture use-specified exceptions and retry in operations on multiple blobs.


## [0.5.6] - 2021-10-04

- Bug fixes and improvements to GcpBlobUpath.


## [0.5.5] - 2021-09-07

- Bug fix related to GCP.


## [0.5.4] - 2021-09-06

- Implement blob locking for GCP.


## [0.5.3] - 2021-08-22

- Improvements to 'lock' methods.


## [0.5.2] - 2021-08-14

- Skip thread pool when `concurrency <= 1`.
- Support extra dependencies via `options.extras_require` in `setup.cfg`.


## [0.5.1] - 2021-07-22

- Reworked `AzureBlobUpath.lock` and `AzureBlobUpath.a_lock`.


## [0.5.0] - 2021-07-19

- `remove_file` loses argument `missing_ok`. Return 0 if file is not found.
- `GcpBlobUpath` copy, download, upload, rename.
- `BlobUpath` gets more methods for download and upload, which are thin wrappers
  around export and import methods.


## [0.4.9] - 2021-07-18

- Another round of API fine tuning. There are some improvements to naming,
  consistency, and simplicity.
- `AzureBlobUpath` has custom blob copying, uploading, downloading.
- Improvements to tests.


## [0.4.5] - 2021-07-10

- Remove `AzureBlobUpath` and `GcpBlobUpath` from package `__init__.py`,
  making their dependencies optional in a future release.


## [0.4.3] - 2021-07-04

- More "native" implementation of async methods.


## [0.4.0] - 2021-07-03

- API refinements.


## [0.3.7] - 2021-06-26

- Bug fix.


## [0.3.4] - 2021-06-23

- Add implementations for Azure and GCP blob stores.


## [0.2.2] - 2021-06-08

- Add JSON and pickle convenience methods to API.


## [0.2.1] - 2021-06-05

- Add preliminary `lock` API.


## [0.2.0] - 2021-05-31

- API iterations.
- `LocalUpath` implementation


## [0.1.0] - 2021-05-24

First draft of API and LocalUpath.
