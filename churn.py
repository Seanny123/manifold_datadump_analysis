from datetime import timedelta

import pandas as pd
from tqdm import tqdm


def calculate_most_recent_streak(betting_dates):
    """
    Calculate the most recent daily streak ending before or on the end_date.

    A daily streak is consecutive days where the user placed at least one bet.
    """
    # Work backwards from the most recent betting date
    betting_dates = sorted(betting_dates, reverse=True)

    # Find the most recent streak
    streak_length = timedelta(days=0)
    for prev_date, curr_date in zip(betting_dates, betting_dates[1:]):
        delta = prev_date - curr_date
        if delta <= timedelta(days=1):
            streak_length += delta
        else:
            break

    # Convert to days
    return streak_length.total_seconds() // (24 * 3600)


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
        days_since_last_best = (last_bet_time - pd.to_datetime("2024-07-06")).days

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

        num_contracts_created_last_7_days = len(
            contracts[contracts["creatorId"] == user_id][
                contracts["createdTime"] >= last_bet_time - pd.Timedelta(days=7)
            ]
        )
        num_contracts_created_last_30_days = len(
            contracts[contracts["creatorId"] == user_id][
                contracts["createdTime"] >= last_bet_time - pd.Timedelta(days=30)
            ]
        )

        comments_last_7_days = comments[
            (comments["userId"] == user_id)
            & (comments["createdTime"] >= last_bet_time - pd.Timedelta(days=7))
        ]
        comments_last_30_days = comments[
            (comments["userId"] == user_id)
            & (comments["createdTime"] >= last_bet_time - pd.Timedelta(days=30))
        ]
        num_comment_likes_last_7_days = comments_last_7_days["likes"].sum()
        num_comment_likes_last_30_days = comments_last_30_days["likes"].sum()
        num_comments_last_7_days = len(comments_last_7_days)
        num_comments_last_30_days = len(comments_last_30_days)

        betting_days = user_data["createdTime"].sort_values().dt.date.nunique()
        daily_streak = calculate_most_recent_streak(user_data["createdTime"])

        results.append(
            {
                "userId": user_id,
                "lastBetTime": last_bet_time,
                "daysSinceLastBet": days_since_last_best,
                "bettingDaysLast7Days": betting_days_last_7_days,
                "numBetsLast7Days": num_bets_last_7_days,
                "bettingDaysLast30Days": betting_days_last_30_days,
                "numBetsLast30Days": num_bets_last_30_days,
                "numContractsCreatedLast7Days": num_contracts_created_last_7_days,
                "numContractsCreatedLast30Days": num_contracts_created_last_30_days,
                "numCommentLikesLast7Days": num_comment_likes_last_7_days,
                "numCommentLikesLast30Days": num_comment_likes_last_30_days,
                "numCommentsLast7Days": num_comments_last_7_days,
                "numCommentsLast30Days": num_comments_last_30_days,
                "bettingDays": betting_days,
                "dailyStreak": daily_streak,
            }
        )

    pd.DataFrame(results).to_parquet("manifold_datasets/churn.parquet")


if __name__ == "__main__":
    main()
