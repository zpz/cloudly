# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).


## [0.1.0] - in progress

- Merged package `upathlib` into `cloudly`. For change log of the `upathlib` code up to this point, see the [original `upathlib` repo](https://github.com/zpz/upathlib)/


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

