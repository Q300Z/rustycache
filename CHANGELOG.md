# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.0](https://github.com/Q300Z/rustycache/compare/v1.0.0...v2.0.0) - 2026-02-05

### Added

- make tokio optional via 'async' feature gate

### Fixed

- feature-gate async example to support no-default-features builds
- update tests to support conditional cleaner startup and improve robustness

### Other

- apply rustfmt to the entire codebase
- implement automated versioning and crates.io publishing via release-plz and OIDC
- final README formatting and cleanup
- apply clippy suggestions for idiomatic code (collapsible_if, or_default)
- optimize hashing with BuildHasher::hash_one and resolve black_box deprecation
- update rust-version to 1.93 and update CI triggers
- add sync and async examples and refine façade for Borrow support
- implement u32 indices and RwLock with batching for concurrent throughput
- update README with record performance results and index-based arena details
- implement Borrow support for zero-allocation cache lookups
- implement index-based arena for LRU/FIFO to eliminate key cloning
- modernize CI pipeline and update documentation with performance results
- implement sharded generic architecture and static dispatch
- optimize LRU/FIFO to O(1) and add performance benchmarks
