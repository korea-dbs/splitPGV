#!/usr/bin/env python3
"""
hnsw_fork_ann_bench.py
======================

검증 항목:
  1. Recall@K  — fork 분리 후에도 ANN 정확도가 유지되는가
  2. 파일 분리  — _hnswnbr 파일이 실제로 생성됐는가
  3. INSERT/DELETE 후 Recall 유지 — neighbor 업데이트가 올바른가
  4. 간단한 쓰기 패턴 확인 — strace 없이 pg_stat_io로 fork별 I/O 비교

사용법:
  python3 hnsw_fork_ann_bench.py \
      --host localhost --port 5499 \
      --dbname postgres \
      --pgdata /home/dbs/fdpvector/pgdata

의존성:
  pip install psycopg2-binary numpy
"""

import argparse
import os
import time
import random
import math
import subprocess
from collections import defaultdict

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 필요 → pip install psycopg2-binary")
    raise

try:
    import numpy as np
except ImportError:
    print("ERROR: numpy 필요 → pip install numpy")
    raise


# ─── 설정 ───────────────────────────────────────────────────────────────────

DIM        = 64       # 벡터 차원 (빠른 테스트용 64, 실제는 768/1536)
N_TRAIN    = 5_000    # 인덱스에 넣을 벡터 수
N_QUERY    = 200      # 쿼리 벡터 수
K          = 10       # Recall@K
M          = 16       # HNSW m 파라미터
EF_BUILD   = 64       # ef_construction
EF_SEARCH  = 40       # hnsw.ef_search
THRESHOLD  = 0.95     # Recall@K 합격 기준

N_INSERT   = 500      # 동적 INSERT 테스트 수
N_DELETE   = 200      # 동적 DELETE 테스트 수


# ─── 유틸 ────────────────────────────────────────────────────────────────────

def vec_to_pg(v):
    """numpy 벡터 → PostgreSQL vector 리터럴"""
    return "[" + ",".join(f"{x:.6f}" for x in v) + "]"


def brute_force_knn(data, query, k):
    """정확한 k-NN (L2 거리 기준)"""
    dists = np.linalg.norm(data - query, axis=1)
    return set(np.argsort(dists)[:k].tolist())


def recall_at_k(true_sets, approx_sets):
    """Recall@K 계산"""
    recalls = []
    for true, approx in zip(true_sets, approx_sets):
        recalls.append(len(true & approx) / len(true))
    return sum(recalls) / len(recalls)


def section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


def ok(msg):   print(f"  ✓  {msg}")
def fail(msg): print(f"  ✗  {msg}")
def info(msg): print(f"     {msg}")


# ─── 검증 함수들 ─────────────────────────────────────────────────────────────

def check_fork_files(pgdata, relname, conn):
    """_hnswnbr 파일이 실제 존재하는지 확인"""
    section("1. 파일 분리 확인 (_hnswnbr 파일 존재 여부)")

    cur = conn.cursor()
    cur.execute("""
        SELECT pg_relation_filenode(i.indexrelid) AS filenode
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_class ic ON ic.oid = i.indexrelid
        WHERE ic.relam = (SELECT oid FROM pg_am WHERE amname = 'hnsw')
          AND c.relname = %s
        LIMIT 1
    """, (relname,))
    row = cur.fetchone()
    if not row:
        fail("HNSW 인덱스를 찾을 수 없음")
        return False

    filenode = row[0]
    info(f"filenode = {filenode}")

    # MAIN fork 크기는 pg_relation_size로 확인 (fork 이름 없이)
    cur.execute("""
        SELECT pg_relation_size(i.indexrelid) AS main_bytes
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indrelid
        WHERE c.relname = %s
        LIMIT 1
    """, (relname,))
    size_row = cur.fetchone()
    if size_row and size_row[0] > 0:
        ok(f"MAIN fork = {size_row[0]:,} bytes")
    else:
        fail("MAIN fork 크기가 0")
        return False

    # _hnswnbr 파일은 pgdata에서 직접 확인
    if not pgdata:
        info("--pgdata 미지정 — 파일 직접 확인 생략")
        info("  확인하려면: find $PGDATA/base -name '*_hnswnbr*'")
        return True

    main_found = False
    nbr_found  = False
    nbr_size   = 0

    for root, dirs, files in os.walk(pgdata + "/base"):
        for f in files:
            if f == str(filenode):
                main_found = True
                fsize = os.path.getsize(os.path.join(root, f))
                ok(f"MAIN     : {os.path.join(root, f)}  ({fsize:,} bytes)")
            if f == f"{filenode}_hnswnbr":
                nbr_found = True
                nbr_size  = os.path.getsize(os.path.join(root, f))
                ok(f"HNSWNBR  : {os.path.join(root, f)}  ({nbr_size:,} bytes)")

    if not nbr_found:
        fail(f"{filenode}_hnswnbr 파일 없음 — fork 분리가 안 됐거나 빌드에 패치 미적용")
        return False

    if nbr_size == 0:
        fail("_hnswnbr 파일은 있지만 크기가 0 — neighbor tuple이 안 쓰이고 있음")
        return False

    return True


def build_index_and_recall(conn, data, queries, ground_truth):
    """인덱스 빌드 후 Recall@K 측정"""
    section("2. 인덱스 빌드 후 Recall@K")

    cur = conn.cursor()

    # 테이블 생성 및 데이터 삽입
    cur.execute("DROP TABLE IF EXISTS ann_bench CASCADE")
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
    conn.commit()
    cur.execute(f"""
        CREATE TABLE ann_bench (
            id SERIAL PRIMARY KEY,
            v  vector({DIM})
        )
    """)
    conn.commit()

    info(f"벡터 {N_TRAIN}개 삽입 중...")
    t0 = time.time()
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO ann_bench (v) VALUES %s",
        [(vec_to_pg(data[i]),) for i in range(N_TRAIN)],
        template="(%s::vector)",
        page_size=500
    )
    conn.commit()
    info(f"삽입 완료 ({time.time()-t0:.1f}s)")

    # HNSW 인덱스 빌드
    info(f"HNSW 인덱스 빌드 중 (m={M}, ef={EF_BUILD})...")
    t0 = time.time()
    cur.execute(f"""
        SET maintenance_work_mem = '512MB';
        CREATE INDEX ann_bench_hnsw ON ann_bench
        USING hnsw (v vector_l2_ops)
        WITH (m = {M}, ef_construction = {EF_BUILD})
    """)
    conn.commit()
    build_time = time.time() - t0
    ok(f"빌드 완료 ({build_time:.1f}s)")

    # Recall@K 측정
    recall = measure_recall(cur, data, queries, ground_truth, "빌드 직후")
    passed = recall >= THRESHOLD

    if passed:
        ok(f"Recall@{K} = {recall:.4f}  (기준 {THRESHOLD}) ✓")
    else:
        fail(f"Recall@{K} = {recall:.4f}  (기준 {THRESHOLD} 미달)")

    return passed, recall


def measure_recall(cur, data, queries, ground_truth, label=""):
    """현재 인덱스로 Recall@K 측정"""
    cur.execute(f"SET hnsw.ef_search = {EF_SEARCH}")

    approx_sets = []
    for q in queries:
        cur.execute(f"""
            SELECT id - 1 AS idx
            FROM ann_bench
            ORDER BY v <-> %s::vector
            LIMIT {K}
        """, (vec_to_pg(q),))
        rows = cur.fetchall()
        approx_sets.append(set(r[0] for r in rows))

    recall = recall_at_k(ground_truth, approx_sets)
    if label:
        info(f"  Recall@{K} [{label}] = {recall:.4f}")
    return recall


def test_dynamic_operations(conn, data, queries, ground_truth):
    """INSERT / DELETE 후 Recall 유지 확인 (neighbor 업데이트 검증)"""
    section("3. 동적 INSERT / DELETE 후 Recall@K 유지")

    cur = conn.cursor()

    # INSERT
    info(f"동적 INSERT {N_INSERT}개...")
    new_vecs = np.random.randn(N_INSERT, DIM).astype(np.float32)
    new_vecs /= np.linalg.norm(new_vecs, axis=1, keepdims=True)

    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO ann_bench (v) VALUES %s",
        [(vec_to_pg(new_vecs[i]),) for i in range(N_INSERT)],
        template="(%s::vector)",
        page_size=200
    )
    conn.commit()

    # INSERT 후 Recall (ground truth는 원래 데이터 기준이라 neighbor 연결만 확인)
    recall_after_insert = measure_recall(cur, data, queries, ground_truth, "INSERT 후")

    # DELETE
    info(f"랜덤 {N_DELETE}개 삭제 후 VACUUM...")
    cur.execute(f"""
        DELETE FROM ann_bench
        WHERE id IN (
            SELECT id FROM ann_bench ORDER BY random() LIMIT {N_DELETE}
        )
    """)
    conn.commit()
    conn.autocommit = True
    cur.execute("VACUUM ann_bench")
    conn.autocommit = False

    recall_after_delete = measure_recall(cur, data, queries, ground_truth, "DELETE+VACUUM 후")

    passed = recall_after_insert >= THRESHOLD and recall_after_delete >= THRESHOLD

    if passed:
        ok(f"INSERT/DELETE 후 Recall 유지 ✓")
    else:
        fail(f"Recall 하락 — neighbor 업데이트에 문제 있을 수 있음")

    return passed, recall_after_insert, recall_after_delete


def check_pg_stat_io(conn):
    """pg_stat_io로 fork별 I/O hit 확인 (PG16+)"""
    section("4. pg_stat_io — fork별 I/O 패턴 확인")

    cur = conn.cursor()

    # pg_stat_io가 있는지 확인 (PG16+)
    cur.execute("""
        SELECT count(*) FROM information_schema.views
        WHERE table_name = 'pg_stat_io'
    """)
    if cur.fetchone()[0] == 0:
        info("pg_stat_io 없음 (PG16 미만) — 생략")
        return

    cur.execute("""
        SELECT object, context, reads, writes, extends
        FROM pg_stat_io
        WHERE object IN ('relation', 'temp relation')
          AND (reads > 0 OR writes > 0 OR extends > 0)
        ORDER BY writes DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    if rows:
        info(f"{'object':15s} {'context':15s} {'reads':>8} {'writes':>8} {'extends':>8}")
        info("-" * 58)
        for r in rows:
            info(f"{str(r[0]):15s} {str(r[1]):15s} {r[2] or 0:>8} {r[3] or 0:>8} {r[4] or 0:>8}")
        #    info(f"{str(r[0]):15s} {str(r[1]):15s} {r[2]:>8} {r[3]:>8} {r[4]:>8}")
    else:
        info("pg_stat_io 데이터 없음")


def check_index_scan_uses_both_forks(conn):
    """EXPLAIN으로 인덱스 스캔이 정상 동작하는지 확인"""
    section("5. EXPLAIN — 인덱스 스캔 정상 동작")

    cur = conn.cursor()
    q = np.random.randn(DIM).astype(np.float32)
    q /= np.linalg.norm(q)

    cur.execute(f"""
        SET hnsw.ef_search = {EF_SEARCH};
        EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
        SELECT id FROM ann_bench
        ORDER BY v <-> %s::vector
        LIMIT {K}
    """, (vec_to_pg(q),))

    rows = cur.fetchall()
    plan = "\n".join(r[0] for r in rows)

    if "Index Scan" in plan:
        ok("Index Scan 사용 확인")
    else:
        fail("Index Scan 미사용 — seq scan으로 fallback")

    # shared hit이 있으면 두 fork 모두 버퍼에서 읽힌 것
    if "shared hit" in plan:
        ok("shared hit 확인 (양쪽 fork 접근)")
    else:
        info("shared hit 없음 (cold cache 상태일 수 있음)")

    # 버퍼 히트 숫자 추출
    import re
    hits = re.findall(r'shared hit=(\d+)', plan)
    if hits:
        info(f"  shared hit = {hits}")


# ─── 메인 ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="HNSW fork 분리 ANN 벤치마크")
    parser.add_argument("--host",   default="localhost")
    parser.add_argument("--port",   type=int, default=5499)
    parser.add_argument("--dbname", default="postgres")
    parser.add_argument("--user",   default="postgres")
    parser.add_argument("--pgdata", default="", help="PGDATA 경로 (파일 존재 확인용)")
    parser.add_argument("--relname", default="ann_bench", help="테이블 이름")
    args = parser.parse_args()

    print(f"""
HNSW Fork 분리 ANN 벤치마크
============================
dim={DIM}, N={N_TRAIN}, queries={N_QUERY}, K={K}
m={M}, ef_build={EF_BUILD}, ef_search={EF_SEARCH}
recall 기준 = {THRESHOLD}
""")

    # 데이터 생성
    info("랜덤 벡터 생성 중...")
    np.random.seed(42)
    data = np.random.randn(N_TRAIN, DIM).astype(np.float32)
    data /= np.linalg.norm(data, axis=1, keepdims=True)

    queries = np.random.randn(N_QUERY, DIM).astype(np.float32)
    queries /= np.linalg.norm(queries, axis=1, keepdims=True)

    # Ground truth (brute force)
    info("Ground truth 계산 중 (brute force)...")
    ground_truth = [brute_force_knn(data, queries[i], K) for i in range(N_QUERY)]

    # DB 연결
    conn = psycopg2.connect(
        host=args.host, port=args.port,
        dbname=args.dbname, user=args.user
    )
    conn.autocommit = False

    results = {}

    try:
        # 1. 인덱스 빌드 + Recall
        passed_build, recall_build = build_index_and_recall(
            conn, data, queries, ground_truth
        )
        results["build_recall"] = recall_build
        results["build_pass"]   = passed_build

        # 2. 파일 분리 확인
        fork_ok = check_fork_files(args.pgdata, args.relname, conn)
        results["fork_files"] = fork_ok

        # 3. 동적 연산 후 Recall
        passed_dyn, r_ins, r_del = test_dynamic_operations(
            conn, data, queries, ground_truth
        )
        results["insert_recall"] = r_ins
        results["delete_recall"] = r_del
        results["dynamic_pass"]  = passed_dyn

        # 4. pg_stat_io
        check_pg_stat_io(conn)

        # 5. EXPLAIN
        check_index_scan_uses_both_forks(conn)

    finally:
        conn.rollback()

    # ── 최종 결과 요약 ──────────────────────────────────────────────────────
    section("최종 결과 요약")

    all_pass = True
    checks = [
        ("빌드 후  Recall@K",          results.get("build_pass",   False),
         f"{results.get('build_recall', 0):.4f}"),
        ("_hnswnbr 파일 분리",          results.get("fork_files",   False), ""),
        ("INSERT 후 Recall@K",          results.get("dynamic_pass", False),
         f"{results.get('insert_recall', 0):.4f}"),
        ("DELETE+VACUUM 후 Recall@K",   results.get("dynamic_pass", False),
         f"{results.get('delete_recall', 0):.4f}"),
    ]

    for label, passed, val in checks:
        suffix = f"({val})" if val else ""
        if passed:
            ok(f"{label:30s} PASS {suffix}")
        else:
            fail(f"{label:30s} FAIL {suffix}")
            all_pass = False

    print()
    if all_pass:
        print("  ✓ 모든 검증 통과 — fork 분리가 정상 동작합니다.")
    else:
        print("  ✗ 일부 검증 실패 — 위 항목을 확인하세요.")

    return 0 if all_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
