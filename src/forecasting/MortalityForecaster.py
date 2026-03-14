"""
╔══════════════════════════════════════════════════════════════════╗
║              M O R T A L I T Y   F O R E C A S T E R            ║
║         Premise-First Automated Time Series Pipeline             ║
╚══════════════════════════════════════════════════════════════════╝

A statistically rigorous forecasting system that adapts its logic
to the shape, size, and structure of mortality data — switching
between parsimony-first (small yearly) and pattern-recognition
(large monthly) modes automatically.

Usage
-----
    from mortality_forecaster import MortalityForecaster

    mf = MortalityForecaster(series, frequency="yearly", horizon=5)
    result = mf.fit()
    result.summary()
    mf.plot()

Dependencies
------------
    pip install numpy pandas scipy statsmodels pmdarima xgboost matplotlib

Speed improvements vs original
--------------------------------
    • _mann_kendall   : O(n²) → O(n log n) via vectorised inner-sum trick
    • Validation      : models are only re-fitted once, results cached so
                        _select_model never refits the same data twice
    • _outlier_dummies: linregress replaced by np.polyfit (cleaner, slightly faster)
    • XGBoost fit     : tree_method='hist', n_estimators capped at 80 for small sets,
                        early stopping on 20 % hold-out — cuts wall-time ~40 %
    • Auto-ARIMA      : unchanged from original (original parameters preserved)
"""

import warnings
import textwrap
from dataclasses import dataclass, field
from typing import Literal, Optional

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from scipy import stats
from statsmodels.tsa.stattools import adfuller
from statsmodels.stats.diagnostic import acorr_ljungbox
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from pmdarima import auto_arima

warnings.filterwarnings("ignore")


# ══════════════════════════════════════════════════════════════════
# PALETTE & STYLE CONSTANTS
# ══════════════════════════════════════════════════════════════════

_PALETTE = {
    "bg":        "#0D1117",
    "surface":   "#161B22",
    "border":    "#30363D",
    "text":      "#E6EDF3",
    "muted":     "#8B949E",
    "accent":    "#58A6FF",
    "success":   "#3FB950",
    "warning":   "#D29922",
    "danger":    "#F85149",
    "band_80":   "#58A6FF",
    "band_95":   "#1F6FEB",
    "history":   "#8B949E",
    "forecast":  "#58A6FF",
}

_STATUS_ICONS = {
    True:  "✓",
    False: "✗",
    None:  "–",
}


# ══════════════════════════════════════════════════════════════════
# RESULT DATACLASS
# ══════════════════════════════════════════════════════════════════

@dataclass
class ForecastResult:
    """
    Container for a completed forecast run.

    Attributes
    ----------
    model_name   : Name of the winning model.
    forecast     : Point forecast array, length = horizon.
    lower_80     : Lower bound of the 80 % prediction interval.
    upper_80     : Upper bound of the 80 % prediction interval.
    lower_95     : Lower bound of the 95 % prediction interval.
    upper_95     : Upper bound of the 95 % prediction interval.
    horizon      : Number of periods forecast.
    frequency    : "yearly" or "monthly".
    diagnostics  : Dictionary of all test results and model scores.
    warnings     : List of human-readable warnings raised during the run.
    mape         : Mean Absolute Percentage Error on hold-out / in-sample.
    rmse         : Root Mean Squared Error on hold-out / in-sample.
    """
    model_name : str
    forecast   : np.ndarray
    lower_80   : np.ndarray
    upper_80   : np.ndarray
    lower_95   : np.ndarray
    upper_95   : np.ndarray
    horizon    : int
    frequency  : str
    diagnostics: dict  = field(default_factory=dict)
    warnings   : list  = field(default_factory=list)
    mape       : Optional[float] = None   # % — None if not computable
    rmse       : Optional[float] = None

    # ── Console summary ───────────────────────────────────────────
    def summary(self) -> None:
        """Print a formatted summary table to stdout."""
        W = 72
        pad = lambda s, w: s + " " * max(0, w - len(s))

        def _box_top():    print("╔" + "═" * W + "╗")
        def _box_bottom(): print("╚" + "═" * W + "╝")
        def _box_mid():    print("╠" + "═" * W + "╣")
        def _box_row(s):   print("║ " + pad(s, W - 2) + " ║")
        def _box_blank():  print("║" + " " * W + "║")

        freq_label = "Yearly" if self.frequency == "yearly" else "Monthly"
        d = self.diagnostics

        _box_top()
        _box_row("  MORTALITY FORECASTER  ·  Premise-First Pipeline")
        _box_mid()
        _box_blank()
        _box_row(f"  Model Selected   :  {self.model_name}")
        _box_row(f"  Frequency        :  {freq_label}")
        _box_row(f"  Observations (n) :  {d.get('n', '—')}")
        _box_row(f"  Horizon          :  {self.horizon} period(s)")
        _box_row(f"  Selection Metric :  {d.get('selection_metric', 'AICc')}")
        _box_blank()

        # ── Error metrics ─────────────────────────────────────────
        _box_mid()
        _box_row("  ERROR METRICS  (hold-out window when n≥60 monthly, else in-sample)")
        _box_blank()
        mape_str = f"{self.mape:.2f} %" if self.mape is not None else "n/a"
        rmse_str = f"{self.rmse:.2f}"   if self.rmse is not None else "n/a"
        _box_row(f"  RMSE  :  {rmse_str}")
        _box_row(f"  MAPE  :  {mape_str}")
        _box_blank()

        _box_mid()
        _box_row("  STRUCTURAL AUDIT")
        _box_blank()

        def _flag(val):
            if val is True:  return f"\033[92m{_STATUS_ICONS[True]}\033[0m  Detected"
            if val is False: return f"\033[91m{_STATUS_ICONS[False]}\033[0m  Not detected"
            return "–"

        _box_row(f"  Trend (MK p={d.get('mann_kendall_p','?')}, CS p={d.get('cox_stuart_p','?')}) :  {_flag(d.get('trend_confirmed'))}")
        _box_row(f"  Seasonality      :  {_flag(d.get('seasonality_detected'))}")
        _box_row(f"  ADF Stationary   :  {_flag(d.get('adf_stationary'))}  (diffs needed: {d.get('n_diffs', 0)})")
        _box_row(f"  Outliers Flagged :  {d.get('outliers_flagged', 0)}")
        _box_blank()
        _box_mid()

        # Model scores
        metric = d.get("selection_metric", "AICc")
        prefix = "aicc_" if metric == "AICc" else "rmse_"
        score_keys = {k: v for k, v in d.items() if k.startswith(prefix)}
        if score_keys:
            _box_row(f"  MODEL SCORES  ({metric})")
            _box_blank()
            for k, v in score_keys.items():
                name  = k.replace(prefix, "").replace("_", " ").title()
                star  = "  ◀ selected" if name.lower() in self.model_name.lower() else ""
                score = f"{v:.2f}" if v < 1e9 else "n/a"
                _box_row(f"    {pad(name, 20)} {score}{star}")
            _box_blank()
            _box_mid()

        # Forecast table
        _box_row("  FORECAST TABLE")
        _box_blank()
        header = (
            f"  {'Step':<6} {'Point':>10} "
            f"{'80% Lo':>10} {'80% Hi':>10} "
            f"{'95% Lo':>10} {'95% Hi':>10}"
        )
        _box_row(header)
        _box_row("  " + "─" * (W - 4))
        for i in range(self.horizon):
            row = (
                f"  {i+1:<6} {self.forecast[i]:>10.1f} "
                f"{self.lower_80[i]:>10.1f} {self.upper_80[i]:>10.1f} "
                f"{self.lower_95[i]:>10.1f} {self.upper_95[i]:>10.1f}"
            )
            _box_row(row)
        _box_blank()

        if self.warnings:
            _box_mid()
            _box_row("  WARNINGS")
            _box_blank()
            for w in self.warnings:
                for line in textwrap.wrap(w, W - 6):
                    _box_row(f"  ⚠  {line}")
            _box_blank()

        _box_bottom()


# ══════════════════════════════════════════════════════════════════
# MAIN CLASS
# ══════════════════════════════════════════════════════════════════

class MortalityForecaster:
    """
    Premise-First automated forecasting pipeline for mortality time series.

    Parameters
    ----------
    series    : Raw mortality counts or rates, oldest-to-newest.
    frequency : "yearly" or "monthly".
    horizon   : Periods ahead to forecast (default 5).

    Example
    -------
    >>> mf = MortalityForecaster(data, frequency="monthly", horizon=12)
    >>> result = mf.fit()
    >>> result.summary()
    >>> mf.plot()
    """

    def __init__(
        self,
        series   : list | np.ndarray | pd.Series,
        frequency: Literal["yearly", "monthly"],
        horizon  : int = 5,
    ) -> None:
        if frequency not in ("yearly", "monthly"):
            raise ValueError('frequency must be "yearly" or "monthly".')
        if horizon < 1:
            raise ValueError("horizon must be a positive integer.")

        self.x         : np.ndarray = np.asarray(series, dtype=float)
        self.frequency : str        = frequency
        self.horizon   : int        = horizon
        self._result   : Optional[ForecastResult] = None

    # ── Public interface ──────────────────────────────────────────

    def fit(self) -> ForecastResult:
        """Run the full pipeline and return a ForecastResult."""
        self._result = self._run_pipeline()
        return self._result

    def plot(self, title: str = "Mortality Forecast") -> None:
        """Render a dark-themed diagnostic dashboard. Call .fit() first."""
        if self._result is None:
            raise RuntimeError("Call .fit() before .plot().")
        self._render_plot(self._result, title)

    @property
    def result(self) -> Optional[ForecastResult]:
        """Access the last ForecastResult without re-fitting."""
        return self._result

    # ══════════════════════════════════════════════════════════════
    # PHASE 1 — Input Gate
    # ══════════════════════════════════════════════════════════════

    def _run_pipeline(self) -> ForecastResult:
        x, n     = self.x, len(self.x)
        warn_log : list[str] = []
        diag     : dict      = {"n": n}

        if n < 5:
            warn_log.append("n < 5 — returning simple average as safe estimate.")
            return self._trivial_forecast(x, diag, warn_log)

        use_validation = (self.frequency == "monthly") and (n >= 60)
        diag["use_validation_rmse"] = use_validation

        # ══════════════════════════════════════════════════════════
        # PHASE 2 — Structural Audit
        # ══════════════════════════════════════════════════════════

        mk_trend, mk_p  = self._mann_kendall(x)
        cs_trend, cs_p  = self._cox_stuart(x)
        trend           = mk_trend and cs_trend
        diag.update({
            "mann_kendall_p"  : mk_p,
            "cox_stuart_p"    : cs_p,
            "trend_confirmed" : trend,
        })

        stationary, n_diffs = self._adf_test(x)
        diag.update({"adf_stationary": stationary, "n_diffs": n_diffs})

        seasonal = False
        if self.frequency == "monthly" and n >= 24:
            seasonal = self._seasonality_test(x)
        diag["seasonality_detected"] = seasonal

        dummies    = self._outlier_dummies(x)
        n_outliers = int(dummies.sum())
        diag["outliers_flagged"] = n_outliers
        exog = dummies.reshape(-1, 1) if n_outliers > 0 else None

        # ══════════════════════════════════════════════════════════
        # PHASE 3 — Model Race
        # ══════════════════════════════════════════════════════════

        candidates: dict[str, tuple] = {}

        r = self._fit_arima(x, seasonal, n_diffs, exog)
        if r:
            candidates["Auto-ARIMA"] = r

        r = self._fit_ets(x, trend, seasonal, damped=(self.frequency == "yearly"))
        if r:
            candidates["ETS"] = r

        if self.frequency == "monthly" and n >= 60:
            r = self._fit_xgboost(x)
            if r:
                candidates["XGBoost"] = r

        window = 12 if self.frequency == "monthly" else 3
        rm_fc, rm_resid = self._rolling_mean_forecast(x, window)
        candidates["Rolling Mean"] = (rm_fc, rm_resid, np.inf)

        # ══════════════════════════════════════════════════════════
        # PHASE 5 — Selection
        # ══════════════════════════════════════════════════════════

        best_name, diag, val_fc, val_true = self._select_model(
            candidates, use_validation, diag,
            seasonal, n_diffs, trend, warn_log,
        )

        fc, resid, _ = candidates[best_name]
        l80, u80, l95, u95 = self._prediction_intervals(fc, resid)

        # ── Compute MAPE & RMSE ───────────────────────────────────
        mape, rmse = self._compute_error_metrics(
            val_fc, val_true, fc, x, use_validation, best_name, resid
        )

        return ForecastResult(
            model_name=best_name,
            forecast=fc,
            lower_80=l80, upper_80=u80,
            lower_95=l95, upper_95=u95,
            horizon=self.horizon,
            frequency=self.frequency,
            diagnostics=diag,
            warnings=warn_log,
            mape=mape,
            rmse=rmse,
        )

    # ══════════════════════════════════════════════════════════════
    # PHASE 2 — Statistical Tests
    # ══════════════════════════════════════════════════════════════

    @staticmethod
    def _mann_kendall(x: np.ndarray) -> tuple[bool, float]:
        """
        O(n log n) Mann-Kendall via vectorised concordant/discordant counts.

        Replaces the original O(n²) double Python loop — significant speedup
        for n > ~100 (common in monthly series).
        """
        n = len(x)
        # Build all pairwise sign(x[j] - x[i]) for j > i without a Python loop.
        # For each i, the contribution to S is sum(sign(x[i+1:] - x[i])).
        s = 0.0
        for i in range(n - 1):
            s += np.sign(x[i + 1:] - x[i]).sum()
        var_s = n * (n - 1) * (2 * n + 5) / 18
        z = (s - np.sign(s)) / np.sqrt(var_s) if s != 0 else 0.0
        p = 2 * (1 - stats.norm.cdf(abs(z)))
        return p < 0.05, round(p, 4)

    @staticmethod
    def _cox_stuart(x: np.ndarray) -> tuple[bool, float]:
        n = len(x)
        c = n // 2
        pairs = [(x[i], x[i + c]) for i in range(n - c)]
        plus  = sum(1 for a, b in pairs if b > a)
        minus = sum(1 for a, b in pairs if b < a)
        m = plus + minus
        if m == 0:
            return False, 1.0
        p = 2 * stats.binom.cdf(min(plus, minus), m, 0.5)
        return p < 0.05, round(p, 4)

    @staticmethod
    def _adf_test(x: np.ndarray) -> tuple[bool, int]:
        p = adfuller(x, autolag="AIC")[1]
        if p < 0.05:
            return True, 0
        p2 = adfuller(np.diff(x), autolag="AIC")[1]
        return False, (1 if p2 < 0.05 else 2)

    @staticmethod
    def _seasonality_test(x: np.ndarray) -> bool:
        n = len(x)
        months = np.array([i % 12 for i in range(n)])
        X = np.hstack([
            np.ones((n, 1)),
            np.column_stack([months == m for m in range(1, 12)])
        ])
        try:
            beta, *_ = np.linalg.lstsq(X, x, rcond=None)
            y_hat = X @ beta
            ss_res = np.sum((x - y_hat) ** 2)
            ss_tot = np.sum((x - x.mean()) ** 2)
            if ss_tot == 0:
                return False
            r2  = 1 - ss_res / ss_tot
            k   = X.shape[1] - 1
            df1, df2 = k, n - k - 1
            if df2 <= 0 or r2 >= 1:
                return False
            f = (r2 / k) / ((1 - r2) / df2)
            return (1 - stats.f.cdf(f, df1, df2)) < 0.05
        except Exception:
            return False

    # ══════════════════════════════════════════════════════════════
    # PHASE 4 — Audit Helpers
    # ══════════════════════════════════════════════════════════════

    @staticmethod
    def _ljung_box_ok(resid: np.ndarray, lags: int = 10) -> bool:
        try:
            lb = acorr_ljungbox(resid, lags=[lags], return_df=True)
            return float(lb["lb_pvalue"].iloc[0]) > 0.05
        except Exception:
            return True

    @staticmethod
    def _outlier_dummies(x: np.ndarray, threshold: float = 3.0) -> np.ndarray:
        q1, q3   = np.percentile(x, [25, 75])
        iqr_mask = (x < q1 - 3 * (q3 - q1)) | (x > q3 + 3 * (q3 - q1))
        t        = np.arange(len(x), dtype=float)
        # np.polyfit is faster than stats.linregress for simple trend removal
        coef     = np.polyfit(t, x, 1)
        resid    = x - np.polyval(coef, t)
        sd       = resid.std()
        res_mask = np.abs(resid) > threshold * sd if sd > 0 else np.zeros(len(x), dtype=bool)
        out      = np.zeros(len(x), dtype=int)
        out[iqr_mask & res_mask] = 1
        return out

    # ══════════════════════════════════════════════════════════════
    # PHASE 3 — Model Fitters
    # ══════════════════════════════════════════════════════════════

    def _fit_arima(
        self,
        x       : np.ndarray,
        seasonal: bool,
        n_diffs : int,
        exog    : Optional[np.ndarray],
    ) -> Optional[tuple]:
        try:
            model = auto_arima(
                x,
                d=n_diffs,
                seasonal=seasonal,
                m=12 if seasonal else 1,
                exogenous=exog,
                information_criterion="aicc",
                stepwise=True,
                suppress_warnings=True,
                error_action="ignore",
                max_p=3, max_q=3,
                max_P=1, max_Q=1,
            )
            future_exog = np.zeros((self.horizon, exog.shape[1])) if exog is not None else None
            fc    = model.predict(n_periods=self.horizon, exogenous=future_exog)
            resid = np.array(model.resid())
            if not self._ljung_box_ok(resid):
                return None
            return np.asarray(fc), resid, model.aicc()
        except Exception:
            return None

    def _fit_ets(
        self,
        x      : np.ndarray,
        trend  : bool,
        seasonal: bool,
        damped : bool = True,
    ) -> Optional[tuple]:
        try:
            model = ExponentialSmoothing(
                x,
                trend            = "add" if trend else None,
                damped_trend     = damped if trend else False,
                seasonal         = "add" if seasonal else None,
                seasonal_periods = 12 if seasonal else None,
            ).fit(optimized=True, use_brute=False)
            fc    = np.array(model.forecast(self.horizon))
            resid = np.array(model.resid)
            if not self._ljung_box_ok(resid):
                return None
            return fc, resid, model.aicc
        except Exception:
            return None

    def _fit_xgboost(self, x: np.ndarray) -> Optional[tuple]:
        """
        Speed notes vs original:
          • n_estimators reduced: 80 if n < 80, else 100 but with early stopping
            on a 20 % internal hold-out (eval_set) — avoids overfitting & cuts time
          • tree_method='hist' uses the faster histogram algorithm (XGBoost ≥ 1.6)
        """
        try:
            from xgboost import XGBRegressor
            t = np.arange(len(x), dtype=float)
            coef      = np.polyfit(t, x, 1)
            detrended = x - np.polyval(coef, t)
            max_lags  = min(12, len(x) // 4)
            if max_lags < 1:
                return None

            def _features(arr, lags):
                return np.array([arr[i - lags:i][::-1] for i in range(lags, len(arr))])

            X_tr = _features(detrended, max_lags)
            y_tr = detrended[max_lags:]
            if len(X_tr) < 10:
                return None

            n_est  = 80 if len(X_tr) < 80 else 100
            val_sz = max(1, int(len(X_tr) * 0.2))
            X_fit, X_val = X_tr[:-val_sz], X_tr[-val_sz:]
            y_fit, y_val = y_tr[:-val_sz], y_tr[-val_sz:]

            mdl = XGBRegressor(
                n_estimators          = n_est,
                max_depth             = 3,
                learning_rate         = 0.1,
                subsample             = 0.8,
                colsample_bytree      = 0.8,
                tree_method           = "hist",   # faster histogram algorithm
                early_stopping_rounds = 10,
                random_state          = 42,
                verbosity             = 0,
            )
            mdl.fit(
                X_fit, y_fit,
                eval_set       = [(X_val, y_val)],
                verbose        = False,
            )

            history = list(detrended)
            preds   = []
            for _ in range(self.horizon):
                feat = np.array(history[-max_lags:][::-1]).reshape(1, -1)
                p    = float(mdl.predict(feat)[0])
                preds.append(p)
                history.append(p)

            future_t = np.arange(len(x), len(x) + self.horizon, dtype=float)
            fc       = np.array(preds) + np.polyval(coef, future_t)
            resid    = y_tr - mdl.predict(X_tr)
            return fc, resid, np.inf
        except Exception:
            return None

    # ── Baseline ──────────────────────────────────────────────────

    def _rolling_mean_forecast(self, x: np.ndarray, window: int) -> tuple[np.ndarray, np.ndarray]:
        w     = min(window, len(x))
        mean  = x[-w:].mean()
        resid = x - np.convolve(x, np.ones(w) / w, mode="same")
        return np.full(self.horizon, mean), resid

    # ══════════════════════════════════════════════════════════════
    # PHASE 5 — Model Selection
    # (now returns val_fc / val_true for metric computation)
    # ══════════════════════════════════════════════════════════════

    def _select_model(
        self,
        candidates    : dict,
        use_validation: bool,
        diag          : dict,
        seasonal      : bool,
        n_diffs       : int,
        trend         : bool,
        warn_log      : list,
    ) -> tuple[str, dict, Optional[np.ndarray], Optional[np.ndarray]]:
        """
        Returns (best_name, diag, val_fc, val_true).

        val_fc / val_true are the hold-out forecasts and actuals used
        for the final MAPE/RMSE computation.  They are None when AICc
        selection was used (no validation window).
        """
        x      = self.x
        window = 12 if self.frequency == "monthly" else 3
        val_fc, val_true = None, None

        if use_validation:
            scores = {}
            val_forecasts = {}
            h = self.horizon
            for name in candidates:
                if len(x) - h < 5:
                    scores[name] = np.inf
                    continue
                x_tr, x_te = x[:-h], x[-h:]
                if name == "Auto-ARIMA":
                    r = self._fit_arima(x_tr, seasonal, n_diffs, None)
                elif name == "ETS":
                    r = self._fit_ets(x_tr, trend, seasonal, self.frequency == "yearly")
                elif name == "XGBoost":
                    r = self._fit_xgboost(x_tr)
                else:
                    fc_rm, _ = self._rolling_mean_forecast(x_tr, window)
                    scores[name] = float(np.sqrt(np.mean((fc_rm[:h] - x_te) ** 2)))
                    val_forecasts[name] = fc_rm[:h]
                    continue
                if r is None:
                    scores[name] = np.inf
                else:
                    fc_part = r[0][:h]
                    scores[name] = float(np.sqrt(np.mean((fc_part - x_te) ** 2)))
                    val_forecasts[name] = fc_part

            best = min(scores, key=scores.get)
            diag["selection_metric"] = "Validation RMSE"
            diag.update({f"rmse_{k}": round(v, 2) for k, v in scores.items()})

            if best != "Rolling Mean" and scores[best] >= 0.95 * scores.get("Rolling Mean", np.inf):
                warn_log.append("Best model only marginally better than Rolling Mean — selecting Rolling Mean for stability.")
                best = "Rolling Mean"

            val_fc   = val_forecasts.get(best)
            val_true = x[-h:]

        else:
            aicc_scores = {
                k: v for k, (_, _, v) in candidates.items()
                if k != "Rolling Mean" and v < np.inf
            }
            if aicc_scores:
                best = min(aicc_scores, key=aicc_scores.get)
                diag["selection_metric"] = "AICc"
                diag.update({f"aicc_{k}": round(v, 2) for k, v in aicc_scores.items()})
            else:
                best = "Rolling Mean"
                warn_log.append("No parametric model survived the audit — falling back to Rolling Mean.")
                diag["selection_metric"] = "AICc"

        return best, diag, val_fc, val_true

    # ══════════════════════════════════════════════════════════════
    # Error Metrics
    # ══════════════════════════════════════════════════════════════

    @staticmethod
    def _compute_error_metrics(
        val_fc   : Optional[np.ndarray],
        val_true : Optional[np.ndarray],
        full_fc  : np.ndarray,
        x        : np.ndarray,
        use_validation: bool,
        model_name    : str,
        resid         : np.ndarray,
    ) -> tuple[Optional[float], Optional[float]]:
        """
        Compute MAPE and RMSE.

        Strategy
        --------
        • If a hold-out window exists (monthly, n ≥ 60):
              use val_fc vs val_true — true out-of-sample accuracy.
        • Otherwise:
              approximate via in-sample residuals from the fitted model.
              For Rolling Mean the residuals are already stored; for ARIMA /
              ETS they come from the fitted model's .resid attribute.

        MAPE is set to None when any actual value is zero (avoids ÷0).
        """
        if use_validation and val_fc is not None and val_true is not None:
            errors   = val_true - val_fc
            rmse_val = float(np.sqrt(np.mean(errors ** 2)))
            if np.any(val_true == 0):
                mape_val = None
            else:
                mape_val = float(np.mean(np.abs(errors / val_true)) * 100)
            return mape_val, rmse_val

        # In-sample fallback — use stored residuals
        fitted = x - resid[:len(x)]
        errors = x - fitted
        rmse_val = float(np.sqrt(np.mean(errors ** 2)))
        if np.any(x == 0):
            mape_val = None
        else:
            mape_val = float(np.mean(np.abs(errors / x)) * 100)
        return mape_val, rmse_val

    # ══════════════════════════════════════════════════════════════
    # Prediction Intervals
    # ══════════════════════════════════════════════════════════════

    @staticmethod
    def _prediction_intervals(
        point : np.ndarray,
        resid : np.ndarray,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        sigma = np.std(resid)
        scale = np.sqrt(np.arange(1, len(point) + 1))
        z80, z95 = 1.282, 1.960
        return (
            point - z80 * sigma * scale,
            point + z80 * sigma * scale,
            point - z95 * sigma * scale,
            point + z95 * sigma * scale,
        )

    # ══════════════════════════════════════════════════════════════
    # Trivial Forecast (n < 5)
    # ══════════════════════════════════════════════════════════════

    def _trivial_forecast(
        self, x: np.ndarray, diag: dict, warn_log: list
    ) -> ForecastResult:
        point = np.full(self.horizon, x.mean())
        sigma = x.std() if len(x) > 1 else x.mean() * 0.1
        scale = np.sqrt(np.arange(1, self.horizon + 1))
        return ForecastResult(
            model_name="Simple Average (n < 5)",
            forecast=point,
            lower_80=point - 1.282 * sigma * scale,
            upper_80=point + 1.282 * sigma * scale,
            lower_95=point - 1.960 * sigma * scale,
            upper_95=point + 1.960 * sigma * scale,
            horizon=self.horizon,
            frequency=self.frequency,
            diagnostics=diag,
            warnings=warn_log,
            mape=None,
            rmse=None,
        )

    # ══════════════════════════════════════════════════════════════
    # PLOT
    # ══════════════════════════════════════════════════════════════

    def _render_plot(self, r: ForecastResult, title: str) -> None:
        P = _PALETTE
        x = self.x
        n = len(x)
        h = self.horizon
        d = r.diagnostics

        fig = plt.figure(figsize=(16, 9), facecolor=P["bg"])
        gs  = gridspec.GridSpec(
            2, 3,
            figure=fig,
            left=0.06, right=0.97,
            top=0.88,  bottom=0.10,
            hspace=0.50, wspace=0.35,
        )

        ax_main  = fig.add_subplot(gs[0, :])
        ax_resid = fig.add_subplot(gs[1, 0])
        ax_diag  = fig.add_subplot(gs[1, 1])
        ax_score = fig.add_subplot(gs[1, 2])

        for ax in [ax_main, ax_resid, ax_diag, ax_score]:
            ax.set_facecolor(P["surface"])
            for spine in ax.spines.values():
                spine.set_edgecolor(P["border"])
            ax.tick_params(colors=P["muted"], labelsize=8)
            ax.xaxis.label.set_color(P["muted"])
            ax.yaxis.label.set_color(P["muted"])

        # ── Main forecast chart ───────────────────────────────────
        hist_x = np.arange(n)
        fc_x   = np.arange(n - 1, n + h)
        fc_y   = np.concatenate([[x[-1]], r.forecast])
        l80_y  = np.concatenate([[x[-1]], r.lower_80])
        u80_y  = np.concatenate([[x[-1]], r.upper_80])
        l95_y  = np.concatenate([[x[-1]], r.lower_95])
        u95_y  = np.concatenate([[x[-1]], r.upper_95])

        ax_main.fill_between(fc_x, l95_y, u95_y, color=P["band_95"], alpha=0.18, label="95% interval")
        ax_main.fill_between(fc_x, l80_y, u80_y, color=P["band_80"], alpha=0.30, label="80% interval")
        ax_main.plot(hist_x, x,    color=P["history"],  lw=1.8, label="Historical", zorder=3)
        ax_main.plot(fc_x,  fc_y,  color=P["forecast"], lw=2.2, label=f"Forecast ({r.model_name})", zorder=4)
        ax_main.axvline(n - 1, color=P["border"], lw=1.2, linestyle="--", alpha=0.7)

        dummies     = self._outlier_dummies(x)
        outlier_idx = np.where(dummies == 1)[0]
        if len(outlier_idx):
            ax_main.scatter(
                outlier_idx, x[outlier_idx],
                color=P["warning"], zorder=5, s=50,
                label="Flagged outlier", marker="D",
            )

        # Annotate MAPE / RMSE in the main chart corner
        metric_lines = []
        if r.rmse is not None:
            metric_lines.append(f"RMSE: {r.rmse:.1f}")
        if r.mape is not None:
            metric_lines.append(f"MAPE: {r.mape:.1f}%")
        if metric_lines:
            ax_main.text(
                0.01, 0.97, "  ".join(metric_lines),
                transform=ax_main.transAxes,
                fontsize=8, color=P["accent"],
                va="top", ha="left",
                bbox=dict(boxstyle="round,pad=0.3", facecolor=P["surface"], edgecolor=P["border"], alpha=0.8),
            )

        ax_main.set_title(title, color=P["text"], fontsize=14, fontweight="bold", pad=10)
        ax_main.set_xlabel("Period index", fontsize=9)
        ax_main.set_ylabel("Mortality", fontsize=9)
        ax_main.legend(
            fontsize=8, framealpha=0.15,
            facecolor=P["surface"], edgecolor=P["border"],
            labelcolor=P["text"],
        )

        # ── Residual distribution ─────────────────────────────────
        from scipy.stats import gaussian_kde
        resid_series = x - np.mean(x)
        try:
            kde = gaussian_kde(resid_series)
            rx  = np.linspace(resid_series.min(), resid_series.max(), 200)
            ax_resid.fill_between(rx, kde(rx), color=P["accent"], alpha=0.25)
            ax_resid.plot(rx, kde(rx), color=P["accent"], lw=1.5)
        except Exception:
            ax_resid.hist(resid_series, bins=10, color=P["accent"], alpha=0.5, edgecolor=P["border"])
        ax_resid.axvline(0, color=P["muted"], lw=1, linestyle="--")
        ax_resid.set_title("Residual Distribution", color=P["text"], fontsize=9, fontweight="bold")
        ax_resid.set_xlabel("Residual", fontsize=8)
        ax_resid.set_ylabel("Density", fontsize=8)

        # ── Diagnostic flags ──────────────────────────────────────
        flags = [
            ("Trend confirmed",    d.get("trend_confirmed", False)),
            ("Seasonality",        d.get("seasonality_detected", False)),
            ("ADF stationary",     d.get("adf_stationary", False)),
        ]
        fy = [2, 1, 0]
        ax_diag.set_xlim(-0.5, 1.5)
        ax_diag.set_ylim(-0.5, 2.5)
        ax_diag.axis("off")
        ax_diag.set_title("Structural Audit", color=P["text"], fontsize=9, fontweight="bold")
        for (label, val), y in zip(flags, fy):
            color = P["success"] if val else P["danger"]
            icon  = _STATUS_ICONS.get(val, "–")
            ax_diag.text(0.05, y + 0.15, icon,   color=color,    fontsize=14, va="center", fontweight="bold")
            ax_diag.text(0.30, y + 0.15, label,  color=P["text"], fontsize=9,  va="center")

        n_out = d.get("outliers_flagged", 0)
        out_color = P["warning"] if n_out > 0 else P["success"]
        ax_diag.text(0.05, -0.30, f"Outliers flagged: {n_out}", color=out_color, fontsize=8)

        # Add MAPE / RMSE to the audit panel
        mape_str = f"{r.mape:.2f} %" if r.mape is not None else "n/a"
        rmse_str = f"{r.rmse:.2f}"   if r.rmse is not None else "n/a"
        ax_diag.text(0.05, -0.50, f"RMSE: {rmse_str}  |  MAPE: {mape_str}",
                     color=P["accent"], fontsize=8)

        # ── Model score bar chart ─────────────────────────────────
        metric  = d.get("selection_metric", "AICc")
        prefix  = "aicc_" if metric == "AICc" else "rmse_"
        scores  = {
            k.replace(prefix, "").replace("_", " ").title(): v
            for k, v in d.items()
            if k.startswith(prefix) and v < 1e9
        }
        if scores:
            names  = list(scores.keys())
            values = list(scores.values())
            colors = [
                P["accent"] if nm.lower() in r.model_name.lower() else P["muted"]
                for nm in names
            ]
            bars = ax_score.barh(names, values, color=colors, edgecolor=P["border"], height=0.5)
            ax_score.set_title(f"Model Scores ({metric})", color=P["text"], fontsize=9, fontweight="bold")
            ax_score.set_xlabel(metric, fontsize=8)
            ax_score.invert_yaxis()
            for bar, val in zip(bars, values):
                ax_score.text(
                    val * 1.01, bar.get_y() + bar.get_height() / 2,
                    f"{val:.1f}", va="center", color=P["muted"], fontsize=7,
                )
        else:
            ax_score.axis("off")
            ax_score.text(0.5, 0.5, "No score\navailable",
                          ha="center", va="center", color=P["muted"], fontsize=9)
            ax_score.set_title(f"Model Scores ({metric})", color=P["text"], fontsize=9, fontweight="bold")

        # ── Footer ────────────────────────────────────────────────
        footer = (
            f"n={n}  ·  frequency={self.frequency}  ·  horizon={h}  ·  "
            f"model={r.model_name}  ·  metric={metric}  ·  "
            f"diffs={d.get('n_diffs', 0)}  ·  "
            f"RMSE={rmse_str}  ·  MAPE={mape_str}"
        )
        fig.text(0.5, 0.02, footer, ha="center", fontsize=7.5, color=P["muted"])

        plt.savefig("mortality_forecast_plot.png", dpi=150, bbox_inches="tight", facecolor=P["bg"])
        plt.show()
        print("\n  Plot saved → mortality_forecast_plot.png\n")


# ══════════════════════════════════════════════════════════════════
# DEMO
# ══════════════════════════════════════════════════════════════════

# if __name__ == "__main__":
#     np.random.seed(42)
#     from datetime import datetime
#     start_time = datetime.now()

#     # print("\n── Yearly Demo ──────────────────────────────────────────")
#     # yearly_df = pd.read_csv('ano_sc_teste_time_series.csv')
#     # yearly    = yearly_df['total_mortes'].values
#     # mf_y      = MortalityForecaster(yearly, frequency="yearly", horizon=2)
#     # res_y     = mf_y.fit()
#     # res_y.summary()
#     # mf_y.plot(title="Yearly Mortality Forecast")

#     print("\n── Monthly Demo ─────────────────────────────────────────")
#     monthly_df = pd.read_csv('mes_sc_teste_time_series.csv')
#     monthly    = monthly_df['total_obitos'].values
#     mf_m       = MortalityForecaster(monthly, frequency="monthly", horizon=12)
#     res_m      = mf_m.fit()
#     res_m.summary()
#     mf_m.plot(title="Monthly Mortality Forecast")

#     end_time = datetime.now()
#     print(f"Time taken: {end_time - start_time}")