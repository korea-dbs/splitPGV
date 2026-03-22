# SplitPGV
SplitPGV physically separates element tuples and neighbor tuples of Heirarchical Nevigatable Small World(HNSW) index into distinct segment files within PosgreSQL's storage manager. SplitPGV ensures that each segment file constains tuples of a single type, thereby improving query throughput(QPS) and increasing buffer cache hit ratio under memory-constrained conditions.
## Getting Started
### Prerequisites
* Ann-Benchmarks

### Build
1. clone our repo
2. build postgres-17.6-splitPGV
