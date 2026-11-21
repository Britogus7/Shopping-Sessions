# Shopping Sessions — High-Performance Rust Engine

A zero-copy, 128-way sharded parallel Rust implementation for the Builders Buildathon **Shopping Sessions** track.

## Open-Source Dependencies

- `ahash` (0.8) — Fast hashing
- `arrow-array`, `arrow-schema`, `parquet` (53) — Apache Arrow / Parquet writer
- `hashbrown` (0.15) — SwissTable hash maps/tables
- `memchr` (2.7) — SIMD-accelerated delimiter scanning
- `memmap2` (0.9) — Memory-mapped read-only CSV input
- `rayon` (1.10) — Data-parallel thread pool

## Build & Run

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
```
