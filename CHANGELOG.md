# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


## [0.3.2] - 2025-01-28

- `cloudly.gcp.bigquery` finetune.


## [0.3.1] - 2025-01-27

- Minor fixes


## [0.3.0] - 2025-01-26

- `cloudly.gcp.bigquery._Table` gets new property `labels` and new method `update_labels`.
- `cloudly.gcp.bigquery` gets new module-level function `table`, analogous to `dataset`.
- Finetuned and enhanced `cloudly.gcp.bigquery.read_streams` with new parameters `num_workers`, `as_dict`, and `queue_cls`.
- `cloudly.biglist.parquet.ParquetBatchData` behavior change: the values produced by `__getitem__` and `__iter__` no longer collapse
  single columns; added attribute `row_as_dict` default to `True`, hence producing dicts by default (as it has been doing up to this release),
  whereas `row_as_dict=False` would produce tuples.
- New module `cloudly.gcp.ai_vector_search` (work in progress).


## [0.2.9] - 2025-01-23

- Finetune `cloudly.gcp.bigquery`.


## [0.2.8] - 2025-01-22


- Enhancements to `cloudly.gcp.bigquery`.


## [0.2.7] - 2025-01-15

- New module `cloudly.gcp.bigquery`.
- More tests.
- New classes `cloudly.utpathlib.serializer.{NewlineDelimitedOrjsonSerializer, CsvSerializer, AvroSerializer}`; all are usable by "biglist".
- `Biglist.new` gets a new parameter `datafile_ext` to specify the extension of the data files.
- New class `cloudly.biglist.ExternalBiglist`, replacing the previous `ParquetBiglist`.
- Removed some backcompat code in `cloudly.biglist` that was introduced in `biglist` versions 0.7.4--0.7.7 (2022-02 - 2023-03).


## [0.2.6] - 2025-01-05

- Fixes and improvements to `cloudly.gcp.{batch, compute, workflows, scheduler}`.


## [0.2.5] - 2025-01-03
- Fixes and improvements to `cloudly.gcp.{batch, compute, workflows, scheduler}`.


## [0.2.4] - 2024-12-30

- Move `versioned_uploadable` from `util` to `upathlib`, keeping a reference in `util` for short-term backcompat.
- Move `ParquetSerializer` from `cloudly.biglist.parquet` to `cloudly.upathlib.serializer`; add methods `read_parquet`, `write_parquet` to `Upath`.
- Finetune log formatting.
- New module `cloudly.gcp.batch`.
- New module `cloudly.gcp.compute`.
- New moudle `cloudly.gcp.scheduler`.
- New module `cloudly.gcp.workflows`.
- New module `cloudly.util.timer`.


## [0.2.3] - 2024-12-22

- Improvements to Sphinx-generated documentation.


## [0.2.2] - 2024-12-22

- Re-arrangements of file org and import paths related to `biglist`, `upathlib`, `serializer`, `parquet`.


## [0.2.1] - 2024-12-21

- Some cleanup and file re-arrangements.


## [0.2.0] - 2024-12-21

- Merged package `biglist` into `cloudly`. For changelog of the `biglist` code up to this point, see the original [`biglist` repo](https://github.com/zpz/biglist).


## [0.1.0] - 2024-12-20

- Merged package `upathlib` into `cloudly`. For changelog of the `upathlib` code up to this point, see the original [`upathlib` repo](https://github.com/zpz/upathlib).
- New module `cloudly.gcp.logging`.


## [0.0.5] - 2024-12-16

- Bug fix related to `datetime.UTC`. This is available in Python 3.11+ while this package requires Python 3.10+. Downgraded the development environment from 3.12 to 3.10 and fixed the bug.


## [0.0.4] - 2024-12-15

- New module `cloudly.util.ratelimit`.
- New module `cloudly.util.logging`.
- New module `cloudly.util.datetime`.
- New module `cloudly.util.docker`.


## [0.0.3] - 2024-11-18

- Bug fix and minor enhancements in `cloudly.gcp.auth`.


## [0.0.2] - 2024-11-17

- Improve import.


## [0.0.1] - 2024-11-17

Initial release.

- GCP credentials and secret manager.

