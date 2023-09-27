# Learn How To Develop And Test Stateful Streaming Data Pipelines

[![CI](https://github.com/mkuthan/stream-processing/actions/workflows/ci.yml/badge.svg)](https://github.com/mkuthan/stream-processing/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/mkuthan/stream-processing/branch/main/graph/badge.svg?token=V9HUU6OJGF)](https://codecov.io/gh/mkuthan/stream-processing)

Shared modules:

* `stream-processing-shared` - shared utilities for developing stateful streaming data pipelines
* `stream-processing-infrastructure` - infrastructure layer with IOs for BigQuery, Pubsub and Cloud Storage
* `stream-processing-test` - shared utilities for testing stateful streaming data pipelines

Use cases:

* `toll-application`, `toll-domain` - sample application for toll data processing,
see [blog post](https://mkuthan.github.io/blog/2023/09/27/unified-batch-streaming/)
* `word-count` - fixed window example,
see [blog post](https://mkuthan.github.io/blog/2022/01/28/stream-processing-part1/)
* `session-window` - session window example,
see [blog post](https://mkuthan.github.io/blog/2022/03/08/stream-processing-part2/)
