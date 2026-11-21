# Shopping Sessions — High-Performance Rust Engine

A zero-copy, SIMD-accelerated, sharded parallel Rust implementation with a native Parquet v1 writer for the Builders Buildathon **Shopping Sessions** track.

## Open-Source Dependencies

- `ahash` (0.8) — Fast hashing
- `hashbrown` (0.15) — SwissTable hash maps/tables (`raw-entry`)
- `memchr` (2.7) — SIMD-accelerated delimiter scanning
- `memmap2` (0.9) — Memory-mapped read-only CSV input
- `mimalloc` (0.1) — Contention-free multi-threaded allocator

Using custom lock-free `std::thread` parallelism, AVX-512/AVX2 + `pclmulqdq` fused `CsvRowSink` CSV scanning, `HugeZeroed`/`HugeSlab` transparent-huge-page (`MADV_HUGEPAGE`) memory, non-temporal (`_mm256_stream_si256`) `NtSlabBuckets`, a 2-pass lock-free DSU for link resolution, a 64-entry L1-resident `FastDateCache` + overlapping `u64` SWAR parsers, pre-faulted zero-contention Stage 4 with Condvar-gated `munmap`, and a native zero-copy `write_vectored` Parquet v1 (`UNCOMPRESSED` + `PLAIN`) writer.

## Build & Run

```sh
docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
```
