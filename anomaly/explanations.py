def explain(dimension, table, column, observed, expected):
    if dimension == "completeness":
        return (
            f"{table}.{column} has higher missing values than expected "
            f"(observed={observed}, expected≈{expected})."
        )

    if dimension == "volume":
        return (
            f"Row count for {table} deviated from baseline "
            f"(observed={observed}, expected≈{expected})."
        )

    if dimension == "distribution":
        return (
            f"Statistical distribution of {table}.{column} shifted "
            f"from historical pattern."
        )

    if dimension == "schema":
        return f"Schema change detected for {table}.{column}."

    if dimension == "referential":
        return f"Foreign key integrity issues detected in {table}.{column}."

    return "Data quality anomaly detected."
