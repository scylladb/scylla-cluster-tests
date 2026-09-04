#!/usr/bin/env python3
"""Generate the tiny local FTS datasets used by ``local_config.yaml``.

The docker-backend FTS run reads everything straight from disk.  The corpora are
generated rather than tracked in git, so run this once after a fresh clone -- the
run itself leaves them in place::

    python3 data_dir/latte/fts_search/generate_local_dataset.py

The output is deterministic (fixed ``SEED``), so regenerating is always safe.

File formats consumed by ``fts.rn``:

* ``shards/documents_NNN.tsv`` -- ``doc_id<TAB>body``
* ``documents.tsv``            -- same, for the non-sharded path
* ``queries_<set>.tsv``        -- ``query_id<TAB>query_text``
* ``qrels_<set>.tsv``          -- ``query_id<TAB>doc_id<TAB>grade``

Two datasets are produced, together covering every branch of
``test_fts_search``:

``local_tiny``
    Synthetic, sharded.  The vocabulary is split into buckets with different
    document frequencies so BM25 has something to rank: ``COMMON`` terms appear
    in most documents, ``MEDIUM`` in some, ``RARE`` in a handful.  Exercises
    ``_parse_shard_spec`` ranges, cumulative doc counts and ``_drop_index``
    between steps.

``local_smoke``
    10 hand-written documents with graded qrels, non-sharded (``documents.tsv``).
    Exercises the multi-dataset loop, the ``documents_file`` branch of
    ``_load_step_shards``, and qrels staging.  Copied from the vector-store
    repo's ``latte/full-text-search/testdata`` smoke fixture.

Every query matches at least one document, so a zero ``result_count`` metric
means a real failure rather than a badly chosen query.
"""

import os
import random

SHARD_COUNT = 3
DOCS_PER_SHARD = 300
BODY_TOKENS = 20
SEED = 20260729

COMMON = ["scylla", "cluster", "node"]
MEDIUM = ["tablets", "compaction", "keyspace", "shard"]
RARE = ["quasar", "obsidian", "zephyr"]
FILLER = [
    "data",
    "write",
    "read",
    "latency",
    "throughput",
    "replica",
    "token",
    "range",
    "memtable",
    "sstable",
    "cache",
    "row",
    "partition",
    "index",
    "query",
    "table",
    "column",
    "value",
    "commitlog",
    "flush",
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATASET_DIR = os.path.join(BASE_DIR, "local_tiny")
SHARDS_DIR = os.path.join(DATASET_DIR, "shards")
SMOKE_DIR = os.path.join(BASE_DIR, "local_smoke")

# 10-document smoke fixture with graded qrels, mirrored from the vector-store
# repo (latte/full-text-search/testdata). Inlined so this repo stays
# self-contained -- it is only ~2 KiB of text.
SMOKE_DOCUMENTS = [
    (
        "doc_001",
        "The Amazon rainforest is often called the lungs of the Earth because it produces vast amounts of oxygen and absorbs carbon dioxide from the atmosphere.",
    ),
    (
        "doc_002",
        "Renewable energy sources like solar and wind power are becoming increasingly cost-effective alternatives to fossil fuels for electricity generation.",
    ),
    (
        "doc_003",
        "The Python programming language is widely used for data science, machine learning, and web development due to its readability and extensive library ecosystem.",
    ),
    (
        "doc_004",
        "Mercury is the smallest planet in our solar system and orbits closest to the Sun, with surface temperatures reaching extreme highs during the day.",
    ),
    (
        "doc_005",
        "Professional basketball players must maintain rigorous training schedules that include strength conditioning, skill drills, and strategic film study.",
    ),
    (
        "doc_006",
        "The invention of the printing press by Johannes Gutenberg in the 15th century revolutionized the distribution of knowledge across Europe.",
    ),
    (
        "doc_007",
        "Mediterranean cuisine emphasizes fresh vegetables, olive oil, and lean proteins, and is associated with numerous health benefits and longevity.",
    ),
    (
        "doc_008",
        "The Great Barrier Reef off the coast of Australia is the largest coral reef system in the world and supports an extraordinary diversity of marine life.",
    ),
    (
        "doc_009",
        "Cloud computing platforms provide on-demand access to computing resources such as virtual machines, storage, and databases over the internet.",
    ),
    (
        "doc_010",
        "Beethoven's Symphony No. 9 is one of the most famous classical compositions and features the Ode to Joy choral finale based on Schiller's poem.",
    ),
]

SMOKE_QUERIES = [
    ("q_001", "what is the amazon rainforest known for"),
    ("q_002", "renewable energy sources compared to fossil fuels"),
    ("q_003", "best programming language for data science"),
    ("q_004", "smallest planet in the solar system"),
    ("q_005", "how do basketball players train professionally"),
    ("q_006", "who invented the printing press"),
    ("q_007", "benefits of mediterranean diet"),
    ("q_008", "where is the great barrier reef located"),
]

SMOKE_QRELS = [
    ("q_001", "doc_001", 3),
    ("q_002", "doc_002", 3),
    ("q_003", "doc_003", 3),
    ("q_003", "doc_009", 1),
    ("q_004", "doc_004", 3),
    ("q_005", "doc_005", 3),
    ("q_006", "doc_006", 3),
    ("q_007", "doc_007", 3),
    ("q_008", "doc_008", 3),
    ("q_008", "doc_001", 1),
]


def _body(rng: random.Random, doc_index: int) -> str:
    """Build a document body with controlled term frequencies."""
    tokens = []
    # COMMON: present in ~90% of documents.
    if doc_index % 10 != 0:
        tokens.append(rng.choice(COMMON))
    # MEDIUM: present in ~30%.
    if doc_index % 10 < 3:
        tokens.append(rng.choice(MEDIUM))
    # RARE: present in ~2%.
    if doc_index % 50 == 0:
        tokens.append(rng.choice(RARE))
    while len(tokens) < BODY_TOKENS:
        tokens.append(rng.choice(FILLER))
    rng.shuffle(tokens)
    return " ".join(tokens)


def write_shards(rng: random.Random) -> int:
    os.makedirs(SHARDS_DIR, exist_ok=True)
    total = 0
    for shard_id in range(SHARD_COUNT):
        path = os.path.join(SHARDS_DIR, f"documents_{shard_id:03d}.tsv")
        with open(path, "w", encoding="utf-8") as f:
            for i in range(DOCS_PER_SHARD):
                doc_index = shard_id * DOCS_PER_SHARD + i
                f.write(f"doc_{doc_index:06d}\t{_body(rng, doc_index)}\n")
        total += DOCS_PER_SHARD
        print(f"wrote {path} ({DOCS_PER_SHARD} docs)")
    return total


def write_queries() -> None:
    os.makedirs(DATASET_DIR, exist_ok=True)

    # Single high-frequency terms -> large result sets, cheap to serve.
    term_common = [(f"q_{i}", term) for i, term in enumerate(COMMON)]

    # Multi-term free-text queries -> more index work per query.
    natural = [
        ("n_0", "scylla cluster node latency"),
        ("n_1", "compaction keyspace throughput"),
        ("n_2", "tablets shard partition range"),
        ("n_3", "quasar obsidian zephyr"),
    ]

    for name, rows in (("term_common", term_common), ("natural", natural)):
        path = os.path.join(DATASET_DIR, f"queries_{name}.tsv")
        with open(path, "w", encoding="utf-8") as f:
            for query_id, text in rows:
                f.write(f"{query_id}\t{text}\n")
        print(f"wrote {path} ({len(rows)} queries)")


def write_smoke_dataset() -> None:
    """Write the non-sharded 10-document fixture with qrels."""
    os.makedirs(SMOKE_DIR, exist_ok=True)

    # No shards/ subdir: local_config.yaml omits `shards` for this dataset, so
    # _load_step_shards falls back to step["documents_file"] ("documents.tsv").
    docs_path = os.path.join(SMOKE_DIR, "documents.tsv")
    with open(docs_path, "w", encoding="utf-8") as f:
        for doc_id, body in SMOKE_DOCUMENTS:
            f.write(f"{doc_id}\t{body}\n")
    print(f"wrote {docs_path} ({len(SMOKE_DOCUMENTS)} docs)")

    queries_path = os.path.join(SMOKE_DIR, "queries_natural.tsv")
    with open(queries_path, "w", encoding="utf-8") as f:
        for query_id, text in SMOKE_QUERIES:
            f.write(f"{query_id}\t{text}\n")
    print(f"wrote {queries_path} ({len(SMOKE_QUERIES)} queries)")

    qrels_path = os.path.join(SMOKE_DIR, "qrels_natural.tsv")
    with open(qrels_path, "w", encoding="utf-8") as f:
        for query_id, doc_id, grade in SMOKE_QRELS:
            f.write(f"{query_id}\t{doc_id}\t{grade}\n")
    print(f"wrote {qrels_path} ({len(SMOKE_QRELS)} qrels)")


def main() -> None:
    rng = random.Random(SEED)
    total = write_shards(rng)
    write_queries()
    print(f"-> {DATASET_DIR}: {total} documents across {SHARD_COUNT} shards\n")

    write_smoke_dataset()
    print(f"-> {SMOKE_DIR}: {len(SMOKE_DOCUMENTS)} documents, non-sharded, with qrels")


if __name__ == "__main__":
    main()
