# Baseline Design Justification — Day 6

## Why a Rolling Window?
A rolling window was chosen instead of static historical baselines to ensure that quality expectations adapt naturally to gradual data evolution. Real-world datasets change due to seasonality, product updates, and upstream system behavior. A fixed baseline quickly becomes stale and causes false alerts. A rolling window balances historical context with recent behavior, keeping thresholds relevant without manual recalibration.

---

## Why Per-Column Baselines?
Data quality issues rarely affect all columns uniformly. Volume, nullability, and distribution characteristics vary significantly by column depending on business semantics and upstream sources. Per-column baselines allow localized detection of anomalies such as a single field becoming sparse or drifting, without masking issues under table-level aggregates. This design improves precision and reduces noisy alerts.

---

## Why Numeric Statistics Instead of Categorical?
Numeric statistics (mean, standard deviation, min, max) provide measurable signals for detecting drift and abnormal variance. Categorical fields often have high cardinality or unstable value sets, making distribution-based baselines brittle and expensive. By focusing first on numeric metrics, the system prioritizes signals that are statistically meaningful, computationally efficient, and easier to operationalize for automated alerting.

---

## What Happens If the Baseline Is Wrong?
If a baseline is incorrect, the system may either miss genuine data quality issues or generate excessive false positives. To mitigate this risk, baseline metrics are not directly enforced as alerts without prior human validation. Baselines are audited for sanity before thresholds are applied, ensuring that automated enforcement is grounded in real-world expectations rather than blind statistical assumptions.

---

## Tradeoffs Knowingly Accepted
- The system initially prioritizes completeness and volume metrics over complex distribution analysis for categorical data.
- Rolling windows may temporarily normalize gradual data degradation if changes occur slowly.
- Numeric distribution monitoring is limited when datasets lack quantitative fields.
- Additional storage is required to maintain historical baseline metrics.

These tradeoffs were accepted to keep the system interpretable, scalable, and aligned with production data engineering constraints, while leaving room for future extensions such as categorical profiling and adaptive thresholding.

---

## Summary
This baseline design favors adaptability, precision, and operational realism. The architecture separates metric generation, baseline construction, and human validation to ensure data quality enforcement is explainable, maintainable, and production-ready.
