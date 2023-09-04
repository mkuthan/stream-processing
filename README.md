# Learn How To Develop And Test Stateful Streaming Data Pipelines

[![CI](https://github.com/mkuthan/stream-processing/actions/workflows/ci.yml/badge.svg)](https://github.com/mkuthan/stream-processing/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/mkuthan/stream-processing/branch/main/graph/badge.svg?token=V9HUU6OJGF)](https://codecov.io/gh/mkuthan/stream-processing)

Modules:

* `stream-processing-test` - shared utilities for testing stateful streaming data pipelines
* `stream-processing-shared` - shared utilities for developing stateful streaming data pipelines
* `stream-processing-infrastructure` - infrastructure layer with basic IOs like BigQuery/Pubsub 
and more specific IOs for handling Dead Letter Queue and pipeline Diagnostic outputs.
* `toll-application`, `toll-domain` - sample application for toll data processing
* `word-count` - fixed window example, see [blog post](http://mkuthan.github.io/blog/2022/01/28/stream-processing-part1/)
* `session-window` - session window example, see [blog post](http://mkuthan.github.io/blog/2022/03/08/stream-processing-part2/)
