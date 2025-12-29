import subprocess
import sys
from pathlib import Path
import psycopg2
from datetime import date
from dotenv import load_dotenv
import os

load_dotenv()

def cleanup_today_anomalies():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME","data_quality_db"),
        user=os.getenv("DB_USER","postgres"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST","localhost"),
        port=os.getenv("DB_PORT", "5432")
    )
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM dq.dq_anomalies WHERE run_date = CURRENT_DATE"
    )
    conn.commit()
    cur.close()
    conn.close()

ROOT = Path(__file__).parent

PIPELINE_STEPS = [
    ROOT / "drift" / "schema_drift_engine.py",
    ROOT / "profiling" / "profiling_engine.py",
    ROOT / "baseline" / "baseline_construction.py",
    ROOT / "baseline" / "baseline_audit.py",
    ROOT / "comparison" / "comparison_engine.py",
    ROOT / "drift" / "distribution_drift_engine.py",
    ROOT / "drift" / "referential_drift_engine.py",
    ROOT / "anomaly" / "anomaly_engine.py",
]

def run_step(script_path: Path):
    print(f"\n▶ Running {script_path}")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        print(f"FAILED: {script_path}")
        print(result.stderr)
        sys.exit(1)

    if result.stdout.strip():
        print(result.stdout.strip())

if __name__ == "__main__":
    print("Starting Data Quality Pipeline")
    cleanup_today_anomalies()

    for step in PIPELINE_STEPS:
        run_step(step)

    print("Pipeline completed successfully")

