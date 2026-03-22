# SplitPGV
SplitPGV physically separates element tuples and neighbor tuples of Heirarchical Nevigatable Small World(HNSW) index into distinct segment files within PosgreSQL's storage manager. SplitPGV ensures that each segment file constains tuples of a single type, thereby improving query throughput(QPS) and increasing buffer cache hit ratio under memory-constrained conditions.
## Getting Started
### Prerequisites
* Ann-Benchmarks (from [ann-benchmarks Github Repository](https://github.com/erikbern/ann-benchmarks/tree/main))
  
### Build
1. Clone the repo.
2. Build postgres-17.6-splitPGV:
```
$ cd splitPGV/postgresql-17.6-splitPGV
$ ./configure --prefix=/opt/splitPGV --without-icu
$ make -j
$ sudo make install
```
3. Install pgvector-splitPGV:
```
$ cd ../pgvector-splitPGV
$ make PG_CONFIG=/opt/splitPGV/bin/pg_config
$ sudo make PG_CONFIG=/opt/pg-32k/bin/pg_config install
```
### Run
1. Make own database, use the path below to make sure sliptPGV work proper.
```
$ mkdir -p /mnt/fdp/data1
$ mkdir -p /mnt/fdp/pg_rel
$ mkdir -p /mnt/fdp/pg_idx
$ sudo chown -R username /mnt/fdp

$ /opt/splitPGV/bin/initdb /mnt/fdp/data1
$ /opt/splitPGV/bin/pg_ctl start -D /mnt/fdp/data1 -l logfile
$ /opt/splitPGV/bin/createdb vdbb1
$ /opt/splitPGV/bin/psql -d vdbb1 -c "CREATE EXTENSION vector;"
```
2. Create tablespace and table named "items":
```
$ /opt/splitPGV/bin/psql -d vdbb1 -c "CREATE TABLESPACE ts_rel LOCATION '/mnt/fdp/pg_rel';"
$ /opt/splitPGV/bin/psql -d vdbb1 -c "CREATE TABLESPACE ts_idx LOCATION '/mnt/fdp/pg_idx';"
$ /opt/splitPGV/bin/psql -d vdbb1 -c "CREATE TABLE items (id INTEGER PRIMARY KEY, embedding vector(#fixed dimension of your vector embedding) TABLESPACE ts_rel;"
```
3. Load Dataset.
4. Create HNSW index with a distance function which fits to the dataset:
```
$ /opt/splitPGV/bin/psql -d vdbb1 -c "CREATE INDEX ON items USING hnsw (embedding vector_cosine_ops) WITH (m = 24, ef_construction = 200) TABLESPACE ts_idx;" 
```
### Evaluate
1. Run ANN-Benchmarks.


Comment out drop existing table and index code in '/ann-benchmarks/ann_benchmarks/algorithms/pgvector/module.py', make sure the benchmark can run on pre-built index.
In case of using dataset not provided from ann-benchmark, add dataset to '/ann-benchmarks/data' and its metadata to '/ann-benchmarks/ann_benchmarks/datasets.py'.
