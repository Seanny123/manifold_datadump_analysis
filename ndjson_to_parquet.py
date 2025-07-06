"""
Written by GPT 4.1 and Cline, which is why the logic is so weird.
"""

import json
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


def stream_ndjson_to_parquet_contracts(ndjson_path, parquet_path):
    fields = [
        ("id", pa.string()),
        ("outcomeType", pa.string()),
        ("groupSlugs", pa.list_(pa.string())),
        ("isResolved", pa.bool_()),
        ("resolution", pa.string()),
        ("resolutionTime", pa.int64()),
        ("closeTime", pa.int64()),
    ]
    schema = pa.schema(fields)
    rows = []
    part_tables = []
    with open(ndjson_path, "r") as f, tqdm(desc="Contracts") as pbar:
        for line in f:
            obj = json.loads(line)
            rows.append(
                [
                    obj["id"],
                    obj["outcomeType"],
                    (
                        obj.get("groupSlugs", [])
                        if isinstance(obj.get("groupSlugs", []), list)
                        else []
                    ),
                    obj["isResolved"],
                    obj.get("resolution", ""),
                    obj.get("resolutionTime", None),
                    obj.get("closeTime", None),
                ]
            )
            if len(rows) >= 10000:
                table = pa.Table.from_pylist(
                    [dict(zip([f[0] for f in fields], r)) for r in rows], schema=schema
                )
                part_tables.append(table)
                rows = []
            pbar.update(1)

    if rows:
        table = pa.Table.from_pylist(
            [dict(zip([f[0] for f in fields], r)) for r in rows], schema=schema
        )
        part_tables.append(table)

    if part_tables:
        big_table = pa.concat_tables(part_tables)
        pq.write_table(big_table, parquet_path)


def stream_ndjson_to_parquet_bets(ndjson_path, parquet_path):
    fields = [
        ("userId", pa.string()),
        ("contractId", pa.string()),
        ("amount", pa.int64()),
        ("outcome", pa.string()),
        ("shares", pa.float64()),
        ("createdTime", pa.int64()),
    ]
    schema = pa.schema(fields)
    rows = []
    part_tables = []
    with open(ndjson_path, "r") as f, tqdm(desc="Bets") as pbar:
        for line in f:
            obj = json.loads(line)
            row = [
                obj["userId"],
                obj["contractId"],
                obj["amount"],
                obj["outcome"],
                float(obj["shares"]),
                obj["createdTime"],
            ]
            rows.append(row)

            if len(rows) >= 10000:
                table = pa.Table.from_pylist(
                    [dict(zip([f[0] for f in fields], r)) for r in rows], schema=schema
                )
                part_tables.append(table)
                rows = []
            pbar.update(1)

    if rows:
        table = pa.Table.from_pylist(
            [dict(zip([f[0] for f in fields], r)) for r in rows], schema=schema
        )
        part_tables.append(table)

    if part_tables:
        big_table = pa.concat_tables(part_tables)
        pq.write_table(big_table, parquet_path)


if __name__ == "__main__":
    stream_ndjson_to_parquet_bets(
        "manifold_datasets/bets.ndjson", "manifold_datasets/bets.parquet"
    )
    stream_ndjson_to_parquet_contracts(
        "manifold_datasets/contracts.ndjson", "manifold_datasets/contracts.parquet"
    )
