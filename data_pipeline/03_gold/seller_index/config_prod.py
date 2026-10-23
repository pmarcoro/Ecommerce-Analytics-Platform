# Production parameters for si1_fact_seller_metrics and si2_fact_seller_index

# Rolling window: 60 days, from 61 to 2 days before the reference date
window_days = 60
lag_days = 2

# Seller index parameters
config_params = {
    # Bayesian smoothing parameters
    "alpha_orders": 8,
    "alpha_reviews": 8,
    "global_late_rate": 0.0907,
    "global_claim_rate": 0.1241,
 
    # Thresholds
    "late_th": 0.01,
    "claim_th": 0.05,
 
    # Maximum penalty caps
    "late_cap": 20,
    "claim_cap": 10,
 
    # Logistic growth rates (k)
    "k_late": 10,
    "k_claim": 10
}
