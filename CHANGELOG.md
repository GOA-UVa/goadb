# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

[//]: # "## [unreleased] - yyyy-mm-dd"

## [0.0.3] - yyyy-mm-dd

### Added

- Common interface `IDataBase` for database backend implementations.
- Support for a `callback_iter` parameter in `insert_dataframe`, called
  after each batch insert with the number of rows processed so far.
- Configurable batch size `chunk_size` for inserts, allowing tuning
  of performance and lock behavior (default 4000).
- Automatic conversion of string `'None'` to `NULL` in SQL queries
  for better compatibility with pandas‑generated queries.
- Created `tests` with simple unit tests for the package.

### Changed
- Logging: replaced `print` statements with standard Python logging.

## [0.0.2] - 2025-06-02

### Added

- SQL DB supports upserts, and now it's the default behaviour, instead of ignore duplicates.

## [0.0.1] - 2025-05-09

Initial version that serves as the baseline for tracking changes in the change log.


[unreleased]: https://gitlab.com/goa-uva/goadb/-/compare/v0.0.3...HEAD
[0.0.3]: https://gitlab.com/goa-uva/goadb/-/compare/v0.0.2...v0.0.3
[0.0.2]: https://gitlab.com/goa-uva/goadb/-/compare/v0.0.1...v0.0.2
[0.0.1]: https://gitlab.com/goa-uva/goadb/-/releases/v0.0.1
