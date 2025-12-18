# Baseline Observations — Day 5  
Date: {{18-12-2025}}

## Overview
A baseline sanity audit was performed on rolling data quality metrics stored in `dq.dq_baseline_stats`. The goal was to validate whether computed baselines align with real-world expectations before introducing automated thresholding.

---

## What Looks Stable
- Average null rate across columns is approximately **0.2%**, indicating generally clean and well-populated data.
- Average record count is around **104,000 rows**, suggesting consistent data volume with no sudden drops or spikes.
- Volume-related metrics appear stable across the rolling window, making them suitable candidates for alert thresholds.

---

## What Fluctuates
- Minor variations in record count are observed but remain within an expected operational range.
- No significant volatility detected in null rates during the baseline window.

---

## Missing or Limited Metrics
- Numeric distribution metrics such as **mean** and **standard deviation** are not present in the baseline results.
- This is due to the dataset primarily containing categorical, identifier, or date-based columns, with few or no numeric fields suitable for distribution analysis.

---

## Potential Failure Risks
- Sudden increases in null rate could indicate upstream ingestion failures or schema changes.
- Significant drops in record count may signal partial loads, pipeline outages, or source-side issues.
- Lack of numeric distribution baselines limits the ability to detect subtle data drift for quantitative fields; this should be addressed if numeric columns are introduced in the future.

---

## Conclusion
Baseline metrics for volume and completeness are realistic and stable, providing a solid foundation for defining automated data quality thresholds. Known limitations around numeric distribution metrics are documented and accepted at this stage of the pipeline.
