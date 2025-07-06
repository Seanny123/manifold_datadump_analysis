Uses `uv` for package and virtual environment management.

Steps for data cleaning:
1. [Download the data dumps](https://docs.manifold.markets/api#trade-history-dumps) into `manifold_datasets/` and unzip them.
2. Use `convert_json_to_ndjson.py` using a JSON streaming Python package, because all tools I've ever used struggle with JSON blobs and I couldn't find a CLI tool that didn't try to load the whole JSON file into RAM.
3. Use `ndjson_to_parquet.py` to extract desired fields into `.parquet` files, because my laptop doesn't have enough RAM load these datasets into memory for analysis.
4. Use `calculate_profits.py` to create a table for analysis, including profits given bets on a market.
