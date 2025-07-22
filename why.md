# Motivation

Trying to predict when a user will stop using Manifold. This is typically called "user churn".

# Hypotheses

Potential predictors:

- Number of markets created in a given time period (7 days before last bet, 30 days before last bet)
- Number of distinct days where a user placed a bet within a given time period (7 days before last bet, 30 days before last bet)
- Number of bets placed in a given time period (7 days before last bet, 30 days before last bet)
- Number of comments created (7 days before last bet, 30 days before last bet)
- The length of the 24-hour streak leading up to their last bet

Bonus predictors I messed up calculating, but would be nice to include if I can fix my bugs fast enough:

- Age of markets being bet on
- Number of bets per market

# Statistical models

## Logistic Regression

Define "inactive" as no bets after 30 days.

Result: Statistically significant model, but "number of bets placed in the last 30 days" is only reliable predictor.

## Linear Regression

Try to predict a user's number of inactive days.

Result: Statistically significant model, but suspicious coefficients.

## Random Forest

Both regression and classification as a sanity check.

Result: Terrible performance and R^2. So it looks like this is actually hard to predict from the given predictors.

# Conclusion

If I had more time, I would try to:

- Calculate profits of bets. A number of people probably stop betting after they've lost all of their balance. This isn't hard, I just haven't looked at Manifold's code carefully enough to make sure I'm not doing this wrong for multiple-choice questions.
- Model the topics of the various markets. A user is probably going to give up visiting Manifold if no markets from their preferred tag. A user might be more likely to stick around if they are willing to bet on a diversity of topics.
- Model market events that a user has bet on. Like whether a market has closed as they predicted. Or whether it closed as N/A.
- Be less strict in selecting users. Current criteria is an account that is more than 30 days old and has placed a bet in the last 120 days. Could get way more examples if I selected all accounts older than 30 days and tried to predict days of inactivity across their history.
- Try harder to compensate for the long-tailed distribution of the metrics.
- Use a chi-squared test to see if the date the last bet was placed on affects likelihood of inactivity.

If I had even more time, I would try to:
- Contact Manifold staff via their Discord to see if they've tried to predict churn.
- Investigate what metrics various Manifold trader bots use.
