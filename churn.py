"""
Create per-user metrics for churn analysis.
"""

from datetime import timedelta

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

    return {
        "bettingDaysLast7Days": betting_days_last_7_days,
        "numBetsLast7Days": num_bets_last_7_days,
        "bettingDaysLast30Days": betting_days_last_30_days,
        "numBetsLast30Days": num_bets_last_30_days,
        "dailyStreak": daily_streak,
    }


def main():
    bets = pd.read_parquet("manifold_datasets/bets.parquet")
    contracts = pd.read_parquet("manifold_datasets/contracts.parquet")
    comments = pd.read_parquet("manifold_datasets/comments.parquet")

    bets["createdTime"] = pd.to_datetime(bets["createdTime"], unit="ms")
    contracts["createdTime"] = pd.to_datetime(contracts["createdTime"], unit="ms")
    comments["createdTime"] = pd.to_datetime(comments["createdTime"], unit="ms")

    results = []

    for user_id, user_data in tqdm(
        bets.sort_values(["userId", "createdTime"]).groupby("userId")
    ):
        last_bet_time = user_data["createdTime"].max()
        days_since_last_best = (pd.to_datetime("2024-07-06") - last_bet_time).days

        contract_metrics = calculate_contract_metrics(
            contracts=contracts, user_id=user_id, last_bet_time=last_bet_time
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
                "lastBetTime": last_bet_time,
                "daysSinceLastBet": days_since_last_best,
                **contract_metrics,
                **commment_metrics,
                **bet_metrics,
            }
        )

    pd.DataFrame(results).to_parquet("manifold_datasets/churn.parquet")


if __name__ == "__main__":
    main()
