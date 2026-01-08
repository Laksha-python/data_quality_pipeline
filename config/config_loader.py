import yaml
import os

CONFIG_PATH = os.getenv(
    "DQ_CONFIG_PATH",
    os.path.join(os.path.dirname(__file__), "dq_thresholds.yaml")
)

def load_thresholds():
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)
