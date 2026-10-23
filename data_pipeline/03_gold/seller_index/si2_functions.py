from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def apply_bayesian_smoothing(df: DataFrame, params: dict, window_days: int) -> DataFrame:
    """Applies Bayesian smoothing to raw rates to handle low volume sellers."""
    return df.withColumn(
        "late_rate_smoothed",
        (F.col(f"late_to_carrier_{window_days}d") + params["alpha_orders"] * params["global_late_rate"]) /
        (F.col(f"delivered_to_carrier_{window_days}d") + params["alpha_orders"])
    ).withColumn(
        "claim_rate_smoothed",
        (F.col(f"claims_{window_days}d") + params["alpha_reviews"] * params["global_claim_rate"]) /
        (F.col(f"reviews_{window_days}d") + params["alpha_reviews"])
    )
    # cancel_rate_smoothed removed (cancellations_60d no longer exists)
 
 
def apply_threshold_and_cap(df: DataFrame, params: dict) -> DataFrame:
    """Ratio between smoothed rates and thresholds, capped."""
    metrics = ["late", "claim"]

    for metric in metrics:
        th = params[f"{metric}_th"]
        cap = params[f"{metric}_cap"]

        col_smoothed = f"{metric}_rate_smoothed"

        # Ratio and Capping
        df = df.withColumn(f"ratio_{metric}", F.col(col_smoothed) / th) \
               .withColumn(f"ratio_{metric}_capped", F.least(F.col(f"ratio_{metric}"), F.lit(cap)))

    return df


def calculate_penalties(df: DataFrame, params: dict) -> DataFrame:
    """Calculates linear, quadratic and logistic penalties using smoothed rates."""
    metrics = ["late", "claim"]   # cancel removed
 
    for metric in metrics:
        th = params[f"{metric}_th"]
        cap = params[f"{metric}_cap"]
        k = params[f"k_{metric}"]
 
        col_raw = f"{metric}_rate"
 
        # Linear Penalty
            # Penalties are triggered only when the raw rate exceeds the threshold,
            # ensuring consistency with the operational targets communicated to sellers.
        df = df.withColumn(
            f"{metric}_capped_linear_penalty",
            F.when(F.col(col_raw) > th, F.col(f"ratio_{metric}_capped") / cap).otherwise(0)
        )
 
        # Quadratic Penalty
        df = df.withColumn(f"{metric}_capped_quadratic_penalty", F.col(f"{metric}_capped_linear_penalty") ** 2)
 
        # Logistic Penalty (Normalization + Sigmoid)
        df = df.withColumn(
            f"{metric}_ratio_capped_normalized",
            (F.col(f"ratio_{metric}_capped") - 1) / (cap - 1)
        ).withColumn(
            f"{metric}_capped_logistic_penalty",
            F.when(
                F.col(f"{metric}_ratio_capped_normalized") > 0,
                1 / (1 + F.exp(-k * (F.col(f"{metric}_ratio_capped_normalized") - 0.5)))
            ).otherwise(0)
        )
 
    return df
 
 
def generate_seller_index(df: DataFrame) -> DataFrame:
    """Calculates final seller indexes using different penalty models."""
    penalty_types = ["linear", "quadratic", "logistic"]
 
    for p_type in penalty_types:
        # 2/3 late, 1/3 reviews (claim). Cancel term removed.
        # Reescaled so max weighted penalty is 4.
        weighted_penalty = (4.0 / 3.0) * (
            2 * F.col(f"late_capped_{p_type}_penalty") +
            F.col(f"claim_capped_{p_type}_penalty")
        ) 
 
        df = df.withColumn(
            f"seller_index_{p_type}",
            F.round(F.greatest(F.lit(1), 5 - weighted_penalty), 0)
        )
    return df
