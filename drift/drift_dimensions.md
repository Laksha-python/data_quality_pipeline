# Drift Dimensions Definition

**Purpose:** Explicitly define what “bad” data looks like so that drift detection logic can be implemented without ambiguity. This document is implementation-ready and does not include code.

---

## 1️⃣ Completeness Drift

**What it is:**
A significant increase in missing (null) values compared to a trusted baseline.

**Metric used:**

* `null_rate = (number of NULL values in column) / (total rows)`

**Baseline:**

* Baseline null_rate is computed from a stable historical window (e.g., first 30 clean runs).

**Comparison logic (percentage change):**

* Calculate percentage increase relative to baseline:

  * `(current_null_rate - baseline_null_rate) / baseline_null_rate`
* Drift is flagged if the increase exceeds a defined threshold.

**Example:**

* Baseline null_rate = 1%
* Today null_rate = 6%
* Increase = 500% → **Completeness drift detected**

**Why this matters:**

* Rising nulls often indicate upstream extraction failures, schema mismatches, or partial ingestion.
* Downstream aggregations and ML features silently degrade when nulls spike.

---

## 2️⃣ Distribution Drift (Numeric)

**What it is:**
A significant change in the statistical distribution of numeric columns.

**Metrics used:**

* Mean
* Standard deviation
* Z-score deviation from baseline

**Formulas (plain English):**

* Mean deviation: compare today’s mean to baseline mean.
* Std deviation spike: compare today’s std dev to baseline std dev.
* Z-score: measure how many standard deviations today’s mean is away from the baseline mean.

**Z-score logic:**

* Z = (current_mean − baseline_mean) / baseline_std_dev

**Acceptable deviation:**

* |Z| ≤ 3 → acceptable
* |Z| > 3 → drift

**Failure example:**

* Baseline mean order value = 500
* Baseline std dev = 50
* Today mean = 700
* Z = (700 − 500) / 50 = 4 → **Distribution drift detected**

---

## 3️⃣ Volume Anomaly

**What it is:**
Unexpected change in total row count compared to baseline volume.

**Metric used:**

* Row count per ingestion run

**Threshold logic:**

* Compare today’s row count with baseline average:

  * Flag drift if row count drops or spikes beyond ±X% (e.g., ±20%).

**Detects:**

* Partial loads
* Duplicate reprocessing
* Silent truncation

**False positive risk:**

* Legitimate seasonality (weekends, holidays)
* Business growth or decline

*Mitigation:* use same-day-of-week or rolling baselines.

---

## 4️⃣ Freshness Degradation

**What it is:**
Increase in data arrival delay over time — not just missing data.

**How delay is computed:**

* `delay = ingestion_time − event_time`
* Track average or P95 delay per run.

**Detection logic:**

* Compare today’s delay against baseline delay.
* Flag if delay increases consistently or breaches a threshold.

**Why delay is dangerous:**

* Data may appear “complete” but is stale.
* Breaks SLAs for dashboards, alerts, and real-time decisions.
* Often precedes total pipeline failure.

---

## 5️⃣ Schema Drift

**What it is:**
Structural changes in the dataset schema.

**Types of schema drift:**

* Column missing
* Column added
* Data type changed

**Severity per change:**

* Column missing → **Critical** (pipeline may break or lose data)
* Type changed → **High** (casts fail, metrics corrupt)
* Column added → **Medium** (safe but may be untracked)

**Detection method:**

* Compare current schema snapshot against baseline schema.

---

## 6️⃣ Referential Decay

**What it is:**
Gradual increase in foreign key (FK) violations over time.

**Metric used:**

* `violation_rate = invalid FK rows / total FK rows`

**How violation rate is calculated:**

* Count rows where FK value does not exist in parent table.
* Divide by total rows containing FK.

**Why gradual increase is critical:**

* Indicates upstream data loss or late-arriving dimensions.
* Slowly corrupts joins and aggregations.
* Often missed because pipeline still “runs successfully.”

---

## ✅ Self-check

* All metrics are clearly defined
* Threshold logic is explicit
* Each drift type explains *why it matters*

**Result:** Another engineer can implement this without follow-up questions.
