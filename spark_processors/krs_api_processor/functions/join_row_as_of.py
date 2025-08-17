from pyspark.sql import SparkSession, functions as F

def join_asof(base_df, join_df, join_cols, prefix=None):
    cols = [F.col(c).alias(f"{prefix}_{c}" if prefix else c) for c in join_cols]
    join_df = join_df.select("company_krs", "valid_from_entry", "valid_to_entry", *cols)

    condition = (
        (base_df.company_krs == join_df.company_krs)
        &
        (join_df.valid_from_entry <= base_df.entry_number)
        &
        ((join_df.valid_to_entry.isNull()) | (join_df.valid_to_entry > base_df.entry_number) )
    )
    return (
        base_df
        .join(join_df, condition, "left")
        .drop(join_df["company_krs"])
        .drop(join_df["valid_from_entry"])
        .drop(join_df["valid_to_entry"])
    )