import sys
import yaml
import os
SUPPORTED_TYPES = {"string", "int", "float", "timestamp", "boolean"}

REQUIRED_COLUMN_KEYS = {"type", "required", "unique"}
REQUIRED_TABLE_KEYS = {
    "source",
    "primary_key",
    "columns",
    "freshness",
    "volume",
}


def fail(msg):
    print(f"CONTRACT INVALID: {msg}")
    sys.exit(1)


def validate_primary_key(table, pk, columns):
    if isinstance(pk, str):
        if pk not in columns:
            fail(f"{table}: primary_key `{pk}` not found in columns")
    elif isinstance(pk, list):
        if not pk:
            fail(f"{table}: composite primary_key is empty")
        for k in pk:
            if k not in columns:
                fail(f"{table}: primary_key column `{k}` not found")
    else:
        fail(f"{table}: primary_key must be string or list")


def validate_columns(table, columns):
    for col, spec in columns.items():
        if not REQUIRED_COLUMN_KEYS.issubset(spec):
            missing = REQUIRED_COLUMN_KEYS - spec.keys()
            fail(f"{table}.{col} missing keys: {missing}")

        if spec["type"] not in SUPPORTED_TYPES:
            fail(f"{table}.{col} has unsupported type `{spec['type']}`")


def validate_foreign_keys(table, columns, all_tables):
    for col, spec in columns.items():
        if "fk" in spec:
            fk = spec["fk"]

            if not isinstance(fk, str) or fk.count(".") != 2:
                fail(f"{table}.{col} invalid fk format `{fk}`")

            ref_table, ref_col = fk.rsplit(".", 1)

            if ref_table not in all_tables:
                fail(f"{table}.{col} FK target table `{ref_table}` not found")

            if ref_col not in all_tables[ref_table]["columns"]:
                fail(f"{table}.{col} FK target column `{ref_col}` not found")

def validate_freshness(table, freshness, columns):
    if not isinstance(freshness, dict):
        fail(f"{table}: freshness must be a dict")

    enabled = freshness.get("enabled", True)

    if not enabled:
        return

    if "timestamp_column" not in freshness:
        fail(f"{table}: freshness.timestamp_column missing")

    ts_col = freshness["timestamp_column"]
    if ts_col not in columns:
        fail(f"{table}: freshness timestamp column `{ts_col}` not in columns")

    time_keys = {"expected_minutes", "expected_hours"}
    delay_keys = {"max_delay_minutes", "max_delay_hours"}

    if not (time_keys & freshness.keys()):
        fail(f"{table}: freshness missing expected SLA")

    if not (delay_keys & freshness.keys()):
        fail(f"{table}: freshness missing max_delay SLA")


def validate_volume(table, volume):
    if not isinstance(volume, dict):
        fail(f"{table}: volume must be a dict")

    if "expected_daily_rows" not in volume:
        fail(f"{table}: volume.expected_daily_rows missing")

    if "variability_percent" not in volume:
        fail(f"{table}: volume.variability_percent missing")


def validate_source(table, source):
    if not isinstance(source, dict):
        fail(f"{table}: source must be a dict")

    if source.get("type") != "csv":
        fail(f"{table}: unsupported source type `{source.get('type')}`")

    path = source.get("path")
    if not path:
        fail(f"{table}: source.path missing")

    if not os.path.isfile(path):
        fail(f"{table}: source CSV not found at `{path}`")

def validate_contract(contract):
    if "tables" not in contract:
        fail("Root key `tables` missing")

    tables = contract["tables"]

    for table, spec in tables.items():
        for key in REQUIRED_TABLE_KEYS:
            if key not in spec:
                fail(f"{table}: missing required key `{key}`")

        validate_source(table, spec["source"])

        columns = spec["columns"]

        validate_primary_key(table, spec["primary_key"], columns)
        validate_columns(table, columns)
        validate_foreign_keys(table, columns, tables)
        validate_freshness(table, spec["freshness"], columns)
        validate_volume(table, spec["volume"])

    print("Contract validation PASSED")


def main(path):
    with open(path, encoding="utf-8") as f:
        contract = yaml.safe_load(f)

    validate_contract(contract)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python contract_validator.py <contract_path>")
        sys.exit(1)

    main(sys.argv[1])
