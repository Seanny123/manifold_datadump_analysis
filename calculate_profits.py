import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from tqdm import tqdm


def calculate_payout(bet, resolution):
    return bet["shares"] if bet["outcome"] == resolution else 0.0


def get_profit_metrics(contract, bets) -> float:
    resolution = contract.get("resolution")
    total_invested = 0
    payout = 0.0
    sale_value = 0
    redeemed = 0

    for bet in bets:
        amount = bet["amount"]
        is_redemption = bet.get("isRedemption", False)
        if is_redemption:
            redeemed += -1 * amount
        elif amount > 0:
            total_invested += amount
        else:
            sale_value -= amount
        payout += calculate_payout(bet, resolution)

    return payout + sale_value + redeemed - total_invested


def main():
    contracts_path = "manifold_datasets/contracts.parquet"
    bets_path = "manifold_datasets/bets.parquet"
    output_path = "manifold_datasets/profits.parquet"

    print("reading contracts")
    contracts_df = pd.read_parquet(contracts_path)
    print("reading bets")
    bets_df = pd.read_parquet(bets_path)
    print("done reading data")

    # Only keep binary contracts
    contracts_df = contracts_df[contracts_df["outcomeType"].str.upper() == "BINARY"]
    # TODO: is getting single entries really that innefficient that we need to convert to dict?
    contracts = contracts_df.set_index("id").to_dict(orient="index")
    valid_contract_ids = set(contracts.keys())

    # Only keep bets for binary contracts
    bets_df = bets_df[bets_df["contractId"].isin(valid_contract_ids)]

    # Group bets by userId and contractId
    grouped = bets_df.groupby(["userId", "contractId"])

    records = []
    for (userId, contractId), group in tqdm(
        grouped, total=len(grouped), desc="Calculating profits"
    ):
        contract = contracts.get(contractId)
        if contract is None:
            continue

        bets = group.to_dict(orient="records")
        profit = get_profit_metrics(contract, bets)

        groupSlugs = contract.get("groupSlugs", [])
        if not isinstance(groupSlugs, list):
            groupSlugs = []

        lastBetTime = int(group["createdTime"].max())

        if (
            contract.get("isResolved", False)
            and contract.get("resolutionTime") is not None
        ):
            resolveTime = int(contract["resolutionTime"])
        else:
            resolveTime = ""

        records.append(
            {
                "userId": userId,
                "contractId": contractId,
                "profit": profit,
                "groupSlugs": groupSlugs,
                "lastBetTime": lastBetTime,
                "resolveTime": resolveTime,
            }
        )

    # Define schema for output
    schema = pa.schema(
        [
            ("userId", pa.string()),
            ("contractId", pa.string()),
            ("profit", pa.float64()),
            ("groupSlugs", pa.list_(pa.string())),
            ("lastBetTime", pa.int64()),
            ("resolveTime", pa.int64()),
        ]
    )

    # Convert to pyarrow Table and write
    table = pa.Table.from_pylist(records, schema=schema)
    pq.write_table(table, output_path)
    print(f"Wrote {len(records)} records to {output_path}")


if __name__ == "__main__":
    main()
