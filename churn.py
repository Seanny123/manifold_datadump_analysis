"""
Create per-user metrics for churn analysis.
"""

from datetime import timedelta

import numpy as np
np.seterr(all="raise")
import pandas as pd
from tqdm import tqdm


def calculate_most_recent_streak(betting_dates):
    """
    Calculate the most recent daily streak.

    A streak is broken once there is a gap of more than 24 hours between consecutive betting dates.
    The streak is counted in days, where each day is considered a full 24-hour period
    regardless of the time of day the bets were placed.
    """
    # Work backwards from the most recent betting date
    betting_dates = sorted(betting_dates, reverse=True)

    # Find the most recent streak
    streak_length = timedelta(days=0)
    for prev_date, curr_date in zip(betting_dates[:-1], betting_dates[1:]):
        delta = prev_date - curr_date
        if delta <= timedelta(days=1):
            streak_length += delta
        else:
            break

    # Convert to days
    return streak_length.total_seconds() // (24 * 3600)


def calculate_commment_metrics(comments, user_id, last_bet_time):
    """
    Calculate comment metrics for a user within the last 7 and 30 days from the last bet time.
    """
    comments_last_7_days = comments[
        (comments["userId"] == user_id)
        & (comments["createdTime"] >= last_bet_time - pd.Timedelta(days=7))
    ]
    comments_last_30_days = comments[
        (comments["userId"] == user_id)
        & (comments["createdTime"] >= last_bet_time - pd.Timedelta(days=30))
    ]

    return {
        "numCommentLikesLast7Days": comments_last_7_days["likes"].sum(),
        "numCommentLikesLast30Days": comments_last_30_days["likes"].sum(),
        "numCommentsLast7Days": len(comments_last_7_days),
        "numCommentsLast30Days": len(comments_last_30_days),
    }


def calculate_contract_metrics(contracts, user_id, last_bet_time):
    """
    Calculate contract creation metrics for a user within the last 7 and 30 days from the last bet time.
    """
    contracts_last_7_days = contracts[
        (contracts["creatorId"] == user_id)
        & (contracts["createdTime"] >= last_bet_time - pd.Timedelta(days=7))
    ]
    contracts_last_30_days = contracts[
        (contracts["creatorId"] == user_id)
        & (contracts["createdTime"] >= last_bet_time - pd.Timedelta(days=30))
    ]

    return {
        "numContractsCreatedLast7Days": len(contracts_last_7_days),
        "numContractsCreatedLast30Days": len(contracts_last_30_days),
    }


def calculate_bet_metrics(user_data, last_bet_time):
    """
    Calculate bet metrics for a user within the last 7 and 30 days from the last bet time.
    """
    bets_last_7_days = user_data[
        (user_data["createdTime"] >= last_bet_time - pd.Timedelta(days=7))
    ]
    betting_days_last_7_days = bets_last_7_days["createdTime"].dt.date.nunique()
    num_bets_last_7_days = len(bets_last_7_days)

    bets_last_30_days = user_data[
        (user_data["createdTime"] >= last_bet_time - pd.Timedelta(days=30))
    ]
    betting_days_last_30_days = bets_last_30_days["createdTime"].dt.date.nunique()
    num_bets_last_30_days = len(bets_last_30_days)

    daily_streak = calculate_most_recent_streak(user_data["createdTime"])

    num_markets_7d = user_data[(user_data["createdTime"] >= last_bet_time - pd.Timedelta(days=7))]["contractId"].nunique()
    bet_counts_7d = user_data[(user_data["createdTime"] >= last_bet_time - pd.Timedelta(days=7))].groupby("contractId").agg(
        betsPerMarket=("createdTime", "count")
    )
    assert len(bet_counts_7d) > 0, "No bet counts data available for the user in the last 7 days."
    assert not bet_counts_7d["betsPerMarket"].dropna().empty, "No bets per market data available for the user in the last 7 days."

    median_bets_per_market_7d = bet_counts_7d["betsPerMarket"].median()
    std_bets_per_market_7d = bet_counts_7d["betsPerMarket"].std()
    min_bets_per_market_7d = bet_counts_7d["betsPerMarket"].min()
    max_bets_per_market_7d = bet_counts_7d["betsPerMarket"].max()

    num_markets_30d = user_data[(user_data["createdTime"] >= last_bet_time - pd.Timedelta(days=30))]["contractId"].nunique()
    bet_counts_30d = user_data[(user_data["createdTime"] >= last_bet_time - pd.Timedelta(days=30))].groupby("contractId").agg(
        betsPerMarket=("createdTime", "count")
    )
    assert len(bet_counts_30d) > 0, "No bet counts data available for the user in the last 30 days."
    assert not bet_counts_30d["betsPerMarket"].dropna().empty, "No bets per market data available for the user in the last 30 days."

    median_bets_per_market_30d = bet_counts_30d["betsPerMarket"].median()
    std_bets_per_market_30d = bet_counts_30d["betsPerMarket"].std()
    min_bets_per_market_30d = bet_counts_30d["betsPerMarket"].min()
    max_bets_per_market_30d = bet_counts_30d["betsPerMarket"].max()

    return {
        "bettingDaysLast7Days": betting_days_last_7_days,
        "numBetsLast7Days": num_bets_last_7_days,
        "bettingDaysLast30Days": betting_days_last_30_days,
        "numBetsLast30Days": num_bets_last_30_days,
        "dailyStreak": daily_streak,
        "numMarkets7Days": num_markets_7d,
        "medianBetsPerMarket7Days": median_bets_per_market_7d,
        "stdBetsPerMarket7Days": std_bets_per_market_7d,
        "minBetsPerMarket7Days": min_bets_per_market_7d,
        "maxBetsPerMarket7Days": max_bets_per_market_7d,
        "numMarkets30Days": num_markets_30d,
        "medianBetsPerMarket30Days": median_bets_per_market_30d,
        "stdBetsPerMarket30Days": std_bets_per_market_30d,
        "minBetsPerMarket30Days": min_bets_per_market_30d,
        "maxBetsPerMarket30Days": max_bets_per_market_30d,
    }


def calculate_market_age_metrics(user_data, contracts, last_bet_time):
    uc = user_data[(user_data["createdTime"] >= last_bet_time - pd.Timedelta(days=7))].merge(contracts, left_on="contractId", right_on="id", how="left", suffixes=("_u", "_c"))
    market_age = uc["createdTime_u"] - uc["createdTime_c"]
    assert len(market_age) > 0, "No market age data available for the user in the last 7 days."
    assert not market_age.dropna().empty, "No market age data available for the user in the last 7 days."
    median_market_age_7d = market_age.median().total_seconds() // (24 * 3600)
    std_market_age_7d = market_age.std().total_seconds() // (24 * 3600)
    min_market_age_7d = market_age.min().total_seconds() // (24 * 3600)
    max_market_age_7d = market_age.max().total_seconds() // (24 * 3600)

    uc = user_data[(user_data["createdTime"] >= last_bet_time - pd.Timedelta(days=30))].merge(contracts, left_on="contractId", right_on="id", how="left", suffixes=("_u", "_c"))
    market_age = uc["createdTime_u"] - uc["createdTime_c"]
    assert len(market_age) > 0, "No market age data available for the last 30 days"
    assert not market_age.dropna().empty, "No market age data available for the last 30 days."
    median_market_age_30d = market_age.median().total_seconds() // (24 * 3600)
    std_market_age_30d = market_age.std().total_seconds() // (24 * 3600)
    min_market_age_30d = market_age.min().total_seconds() // (24 * 3600)
    max_market_age_30d = market_age.max().total_seconds() // (24 * 3600)

    return {
        "medianMarketAgeLast7Days": median_market_age_7d,
        "stdMarketAgeLast7Days": std_market_age_7d,
        "minMarketAgeLast7Days": min_market_age_7d,
        "maxMarketAgeLast7Days": max_market_age_7d,
        "medianMarketAgeLast30Days": median_market_age_30d,
        "stdMarketAgeLast30Days": std_market_age_30d,
        "minMarketAgeLast30Days": min_market_age_30d,
        "maxMarketAgeLast30Days": max_market_age_30d,
    }


def main():
    bets = pd.read_parquet("manifold_datasets/bets.parquet")
    contracts = pd.read_parquet("manifold_datasets/contracts.parquet")
    comments = pd.read_parquet("manifold_datasets/comments.parquet")

    bets["createdTime"] = pd.to_datetime(bets["createdTime"], unit="ms")
    contracts["createdTime"] = pd.to_datetime(contracts["createdTime"], unit="ms")
    comments["createdTime"] = pd.to_datetime(comments["createdTime"], unit="ms")

    bet_times = bets.groupby("userId").agg(
        firstBetTime=("createdTime", "min"), lastBetTime=("createdTime", "max")
    )
    bet_times = bet_times.assign(
        daysSinceFirstBet=lambda x: (
            pd.to_datetime("2024-07-06") - x["firstBetTime"]
        ).dt.days,
        daysSinceLastBet=lambda x: (
            pd.to_datetime("2024-07-06") - x["lastBetTime"]
        ).dt.days,
    )
    valid_user_ids = bet_times[
        ((bet_times["daysSinceFirstBet"] > 30) & (bet_times["daysSinceLastBet"] < 30))
        | ((bet_times["daysSinceLastBet"] > 30) & (bet_times["daysSinceLastBet"] < 120))
    ]

    valid_bets = bets[
        bets["userId"].isin(valid_user_ids.index)
        # filter out bets on markets that were filtered out previously
        # due to being a poll or another weird kind of market type
        & bets["contractId"].isin(contracts["id"].unique())
    ]


    results = []

    for user_id, user_data in tqdm(
        valid_bets.sort_values(["userId", "createdTime"]).groupby("userId")
    ):
        last_bet_time = user_data["createdTime"].max()
        days_since_last_bet = (pd.to_datetime("2024-07-06") - last_bet_time).days

        contract_metrics = calculate_contract_metrics(
            contracts=contracts, user_id=user_id, last_bet_time=last_bet_time
        )

        market_age_metrics = calculate_market_age_metrics(
            user_data=user_data, contracts=contracts, last_bet_time=last_bet_time
        )

        commment_metrics = calculate_commment_metrics(
            comments=comments, user_id=user_id, last_bet_time=last_bet_time
        )

        bet_metrics = calculate_bet_metrics(
            user_data=user_data, last_bet_time=last_bet_time
        )

        results.append(
            {
                "userId": user_id,
                "daysSinceLastBet": days_since_last_bet,
                **contract_metrics,
                **commment_metrics,
                **bet_metrics,
                **market_age_metrics,
            }
        )

        if len(results) % 200 == 0:
            pd.DataFrame(results).to_parquet("manifold_datasets/churn_tmp.parquet")

    pd.DataFrame(results).to_parquet("manifold_datasets/churn_2.parquet")


if __name__ == "__main__":
    main()
