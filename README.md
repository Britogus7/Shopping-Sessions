# Shopping Sessions — High-Performance Rust Engine

A zero-copy, SIMD-accelerated, sharded parallel Rust implementation with a native Parquet v1 writer for the Builders Buildathon **Shopping Sessions** track.

## Open-Source Dependencies

- `ahash` (0.8) — Fast hashing
- `hashbrown` (0.15) — SwissTable hash maps/tables (`raw-entry`)
- `memchr` (2.7) — SIMD-accelerated delimiter scanning
- `memmap2` (0.9) — Memory-mapped read-only CSV input
- `mimalloc` (0.1) — Contention-free multi-threaded allocator

Using custom lock-free `std::thread` parallelism, AVX-512/AVX2/scalar CSV scanning, a 2-pass lock-free DSU for link resolution, zero-realloc `SlabBuckets`, and a native zero-copy Parquet v1 (`UNCOMPRESSED` + `PLAIN`) writer.

## Build & Run

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
```
