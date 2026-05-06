import polars as pl

def compare_dataframes(df1, df2, key=None):
    """
    Returns a dict with all differences between two DataFrames.

    Parameters
    ----------
    df1, df2 : pl.DataFrame
    key      : str or list of str, optional
               Column(s) used to align rows when shapes differ.
               If None and shapes differ, no row‑level value comparison is done.

    Returns
    -------
    dict with keys:
        'same'          : bool, overall equality (True only if perfectly identical)
        'shape'         : tuple of (rows, cols) for both
        'row_count_diff': row count difference (0 if same)
        'columns'       : dict with 'only_df1', 'only_df2', 'common'
        'type_diff'     : dict of col -> (type_df1, type_df2) for common columns with different dtypes
        'value_diff'    : None, or a DataFrame of mismatched rows if comparison was possible
        'message'       : human‑readable summary
    """
    result = {
        "same": False,
        "shape": (df1.shape, df2.shape),
        "row_count_diff": abs(df1.shape[0] - df2.shape[0]),
        "columns": {
            "only_df1": list(set(df1.columns) - set(df2.columns)),
            "only_df2": list(set(df2.columns) - set(df1.columns)),
            "common": list(set(df1.columns) & set(df2.columns)),
        },
        "type_diff": {},
        "value_diff": None,
        "message": "",
    }

    # 1. Schema‑level differences (types)
    common_cols = result["columns"]["common"]
    for col in common_cols:
        t1, t2 = df1.schema[col], df2.schema[col]
        if t1 != t2:
            result["type_diff"][col] = (t1, t2)

    # 2. Quick full equality if shapes and schemas are identical
    if df1.shape == df2.shape and not result["columns"]["only_df1"] \
       and not result["columns"]["only_df2"] and not result["type_diff"]:
        if df1.equals(df2):
            result["same"] = True
            result["message"] = "DataFrames are identical."
            return result
        # else fall through to value comparison

    # 3. Row‑level value comparison
    if df1.shape[0] == df2.shape[0] and common_cols:
        # Same number of rows → compare by position (null‑safe)
        mask_expr = [
            ((df1[col] != df2[col])
             & (df1[col].is_not_null() | df2[col].is_not_null()))
            .alias(col)
            for col in common_cols
        ]
        mask = df1.with_columns(mask_expr).select(common_cols)
        diff_mask = mask.select(pl.any_horizontal("*")).to_series()
        if diff_mask.any():
            diff_indices = diff_mask.arg_true()
            diff_rows = df1[diff_indices].with_row_index("__row__")
            # Also include the "other" values for clarity
            df2_diff = df2[diff_indices].rename(
                {c: f"{c}_other" for c in common_cols}
            )
            result["value_diff"] = pl.concat(
                [diff_rows, df2_diff], how="horizontal"
            )
        else:
            result["value_diff"] = pl.DataFrame()  # empty = no value diff

    elif key and common_cols:
        # Different row counts – join on key to align
        key = [key] if isinstance(key, str) else key
        # Ensure key columns exist in both
        missing_key = [k for k in key if k not in df1.columns or k not in df2.columns]
        if missing_key:
            result["message"] = f"Key column(s) {missing_key} missing in one or both DataFrames."
            return result

        joined = df1.join(df2, on=key, how="outer", suffix="_2")
        # Compare only the common non‑key columns
        value_cols = [c for c in common_cols if c not in key]
        for col in value_cols:
            col2 = f"{col}_2"
            if col2 not in joined.columns:
                continue
            diff_expr = (
                (joined[col] != joined[col2])
                & (joined[col].is_not_null() | joined[col2].is_not_null())
            )
            joined = joined.with_columns(diff_expr.alias(f"__diff_{col}"))
        diff_cols = [f"__diff_{col}" for col in value_cols]
        if diff_cols:
            any_diff = joined.select(pl.any_horizontal(diff_cols)).to_series()
            if any_diff.any():
                result["value_diff"] = joined.filter(any_diff)
            else:
                result["value_diff"] = pl.DataFrame()
        else:
            result["value_diff"] = pl.DataFrame()
    else:
        result["message"] = (
            "Row counts differ and no key was provided – cannot compare values row‑by‑row."
        )
        # Still can note schema diffs above
        if not any([result["columns"]["only_df1"], result["columns"]["only_df2"],
                    result["type_diff"]]):
            result["message"] += " (Column names and types are identical.)"

    # 4. Compose summary message
    if not result["same"]:
        parts = []
        if result["row_count_diff"]:
            parts.append(f"Row count difference: {result['row_count_diff']}")
        if result["columns"]["only_df1"]:
            parts.append(f"Columns only in df1: {result['columns']['only_df1']}")
        if result["columns"]["only_df2"]:
            parts.append(f"Columns only in df2: {result['columns']['only_df2']}")
        if result["type_diff"]:
            parts.append(f"Type mismatches: {result['type_diff']}")
        if isinstance(result["value_diff"], pl.DataFrame) and not result["value_diff"].is_empty():
            parts.append(f"{result['value_diff'].shape[0]} row(s) with value differences.")
        elif result["value_diff"] is not None:
            parts.append("No value differences in common columns.")
        result["message"] = " | ".join(parts) if parts else "No differences found (check message)."
    return result

credentials = {
            'aws_region': 'us-east-1',
            'aws_access_key_id': 'test',
            'aws_secret_access_key': 'test',
            'allow_http': 'true',
            'force_path_style': 'true',
            'aws_endpoint_url': 'http://localhost:9000',
        }

df = pl.read_delta(
         's3://landing/test',
         storage_options=credentials)

df1 = pl.read_csv('/home/noname/mmm/prod-grade/data/source/Reviews.csv')

diff = compare_dataframes(df1.slice(0, 63161), df)
print(diff)
