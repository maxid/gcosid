# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

- **Layout split.** The shared vocabulary moved to `gcosid/core` (generator contracts, the
  segment model, the radix/prefix/date vocabulary, the snowflake `Layout`, `Stat`), and
  machine-id distribution moved to `gcosid/machine`. Both are re-exported from the root
  package as aliases, so existing imports and type assertions are unchanged; `database/`
  and `redis/` import `machine` for the interfaces they implement. Root: 25 files -> 19.

### Added

- `ToString.Parse`, the inverse of `Format`, so a decimal id can be read back.

- Initial scaffold.
