# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


## [0.2.4] - in progress

- Move `versioned_uploadable` from `util` to `upathlib`, keeping a reference in `util` for short-term backcompat.
- Finetune log formatting.
- New module `cloudly.gcp.batch`.


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

