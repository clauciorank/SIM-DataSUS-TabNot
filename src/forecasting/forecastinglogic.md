Fair enough. Here's the full text with Phase 6 trimmed to what actually serves forecasting:

---

**A Robust Automated Forecasting System for Mortality Data**
*Premise-First Workflow — Revised*

---

## Phase 1: The Input Gate (Frequency & Volume)

The system first examines the "shape" of the data to establish the rules of engagement.

**Yearly Path:** If the data is annual, the system assumes a small sample size (n) and activates **Parsimony Mode**, where complexity is penalized heavily to avoid overfitting sparse historical records.

**Monthly Path:** If the data is monthly, the system activates **Seasonal Detection** and **Complexity Modes**, allowing models capable of capturing cyclical patterns.

**The Minimum n Rule:** If n < 5, the system bypasses all modeling and returns either a simple average or the last recorded value as a safe, transparent estimate.

**The Monthly Volume Threshold:** For monthly data, the system additionally checks whether n ≥ 60 (5 years) before permitting hold-out validation. Below this threshold, the system falls back to AICc as the selection metric — because with fewer observations, carving out a validation window produces estimates too noisy to be trustworthy.

---

## Phase 2: The Structural Audit (Statistical Testing)

Before any model is allowed to run, the data is interrogated to determine what structure it can realistically support.

**Step A — The Trend Test:** The system runs both the **Mann-Kendall** and **Cox-Stuart** tests in tandem. Mann-Kendall is the primary decision-maker, as it handles non-normality and small samples better — a common condition in mortality series. Cox-Stuart serves as a corroborating check. A trend is only confirmed if both tests agree. If no trend is found, models are locked to stationary versions, preventing the projection of a "ghost trend" into the future.

**Step B — The Seasonality Test (Monthly Only):** For monthly data, the system tests for a repeating 12-month pattern. If seasonality is absent, seasonal components in ARIMA and ETS are disabled, preserving degrees of freedom for more meaningful parameters.

**Step C — The Stationarity Test (ADF):** The system determines whether the mean and variance are stable over time. This dictates how many times the data must be differenced before it is ready for ARIMA modeling.

---

## Phase 3: The Model Selection Race

Candidates are fitted to the data with specific constraints inherited from Phase 2 — this is not a black box.

**Candidate 1 — Auto-ARIMA:** Searches for autocorrelation structure (how much this period relates to prior ones). Differencing order is set by Phase 2, not left to guesswork. Seasonal terms are only included if Step B confirmed seasonality.

**Candidate 2 — Exponential Smoothing (ETS/Holt):** Focuses on level, trend, and seasonality. For yearly mortality data, a **Damped trend** is preferred by default, reflecting the common demographic reality that growth eventually plateaus rather than compounding indefinitely.

**Candidate 3 — XGBoost (Monthly, n ≥ 60 only):** Enters the race only when the monthly data is large enough to support it. The series is de-trended first, reframing the problem as "predicting the change in deaths" rather than the raw total. Lag features are selected through cross-validated importance scoring rather than fixed windows, to prevent silent overfitting on moderately sized series.

**Candidate 4 — The Baseline (Rolling Mean):** A 3-year or 12-month rolling average. This is the benchmark. Any complex model that cannot meaningfully outperform this is considered a failure.

---

## Phase 4: The Premise Audit (The Disqualification Round)

A low error score does not guarantee survival. Models are disqualified here on structural grounds.

**The Residual Check (Ljung-Box):** The system inspects the model's residuals for remaining patterns. A pattern in the residuals means the model has missed something systematic. The model is rejected.

**The Coefficient Check:** Each parameter in the model is tested for statistical significance. Any model relying on a high p-value term — such as a complex lag that doesn't meaningfully contribute — is rejected as overfit.

**The Outlier Impact:** The system detects observations that deviate sharply from the historical envelope using a combination of residual screening (±3 standard deviations) and an IQR envelope check. Confirmed outliers are assigned an **intervention dummy variable** for that period, preventing their influence from distorting the model's core parameters. The goal here is narrow and practical: protect the forecast from being anchored to an abnormal past, not to classify or explain the shock itself.

---

## Phase 5: Final Deployment Logic

Surviving models are ranked and a final selection is made.

**Selection Metric:** For small or yearly samples (or monthly series with n < 60), ranking is by **AICc**, which penalizes complexity harshly relative to sample size. For larger monthly series, ranking is by **Validation RMSE** on a held-out window.

**The Parsimony Rule:** If the best-performing model's advantage over the Rolling Mean falls within a defined margin, the system selects the Rolling Mean for its robustness and interpretability.

**Prediction Intervals:** The selected model outputs not just a point forecast but **calibrated uncertainty bounds** (80% and 95% intervals). For mortality applications, a projection without confidence intervals is incomplete — decision-makers need to see the range of plausible outcomes, not just the central estimate. Interval coverage is back-tested against the held-out window where data permits.

---

## Summary Table

| Task | Yearly Logic | Monthly Logic |
|---|---|---|
| Trend Detection | Mann-Kendall (primary) + Cox-Stuart (corroboration) | Same, plus differencing or de-trending |
| Seasonality | Ignored — insufficient data | Mandatory 12-month test |
| ML (XGBoost) | Forbidden — too risky for small n | Allowed at n ≥ 60, with cross-validated lag selection |
| Selection Metric | AICc | AICc if n < 60; Validation RMSE if n ≥ 60 |
| Integrity Check | Heavy penalty for extra parameters | Focus on seasonal capture and residual whiteness |
| Outlier Handling | Intervention dummy to protect forecast parameters | Same |
| Output | Point forecast + prediction intervals | Point forecast + prediction intervals |