"""
Written by GPT 4.1 and Cline, which is why the table construction logic is so weird.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm


def stream_ndjson_to_parquet_comments(ndjson_path, parquet_path):
    # TODO: parse the text field, which then becomes the "content" field
    fields = [
        ("id", pa.string()),
        ("userId", pa.string()),
        ("contractId", pa.string()),
        ("likes", pa.int64()),
        ("createdTime", pa.int64()),
    ]
    schema = pa.schema(fields)
    rows = []
    part_tables = []
    with open(ndjson_path, "r") as f, tqdm(desc="Comments") as pbar:
        for line in f:
            obj = json.loads(line)

            row = [
                obj["id"],
                obj["userId"],
                obj.get("contractId", ""),
                obj.get("likes", 0),
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


def stream_ndjson_to_parquet_contracts(ndjson_path, parquet_path):
    fields = [
        ("id", pa.string()),
        ("creatorId", pa.string()),
        ("totalLiquidity", pa.int64()),
        ("volume", pa.int64()),
        ("outcomeType", pa.string()),
        ("groupSlugs", pa.list_(pa.string())),
        ("isResolved", pa.bool_()),
        ("resolution", pa.string()),
        ("createdTime", pa.int64()),
        ("resolutionTime", pa.int64()),
        ("closeTime", pa.int64()),
    ]
    schema = pa.schema(fields)
    rows = []
    part_tables = []
    with open(ndjson_path, "r") as f, tqdm(desc="Contracts") as pbar:
        for line in f:
            obj = json.loads(line)
            if "totalLiquidity" not in obj and obj["outcomeType"] in {
                "QUADRATIC_FUNDING",
                "BOUNTIED_QUESTION",
                "POLL",
            }:
                continue
            rows.append(
                [
                    obj["id"],
                    obj["creatorId"],
                    obj["totalLiquidity"],
                    obj.get("volume", -1),
                    obj["outcomeType"],
                    (
                        obj.get("groupSlugs", [])
                        if isinstance(obj.get("groupSlugs", []), list)
                        else []
                    ),
                    obj["isResolved"],
                    obj.get("resolution", ""),
                    obj["createdTime"],
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
    stream_ndjson_to_parquet_comments(
        "manifold_datasets/comments.ndjson", "manifold_datasets/comments.parquet"
    )
    # stream_ndjson_to_parquet_bets(
    #     "manifold_datasets/bets.ndjson", "manifold_datasets/bets.parquet"
    # )
    # stream_ndjson_to_parquet_contracts(
    #     "manifold_datasets/contracts.ndjson", "manifold_datasets/contracts.parquet"
    # )
