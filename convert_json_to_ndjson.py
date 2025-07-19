import json
from pathlib import Path

import json_stream
import json_stream.dump
from tqdm import tqdm


root = Path("manifold_datasets")


def convert_json_to_ndjson(input_filepath: Path, output_filepath: Path):
    """
    Convert a JSON file to NDJSON format.
    """
    with open(input_filepath, "r") as fi:
        with open(output_filepath, "w") as fo:
            for obj in tqdm(json_stream.load(fi)):
                fo.write(json.dumps(obj, cls=json_stream.dump.JSONStreamEncoder))
                fo.write("\n")


if __name__ == "__main__":
    root = Path("manifold_datasets")
    convert_json_to_ndjson(
        root / "manifold-comments-20240706.json" / "manifold-comments-20240706hi.json",
        root / "comments.ndjson",
    )
    convert_json_to_ndjson(
        root / "manifold-dump-bets-04072024.json" / "bets.json", root / "bets.ndjson"
    )
    convert_json_to_ndjson(
        root / "manifold-comments-20240706.json" / "manifold-comments-20240706hi.json",
        root / "contracts.ndjson",
    )
