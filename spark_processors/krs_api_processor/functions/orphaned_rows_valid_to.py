from pyspark.sql import functions as F

def fix_open_ended_orphaned_entries(df):
    # Distinct start entries per company (the “replacements”)
    starts = (
        df.select(
            F.col("company_krs"),
            F.col("valid_from_entry").alias("_e_from")
        )
        .dropDuplicates()
        .alias("s")
    )

    d = df.alias("d")
    to_dtype = df.schema["valid_to_entry"].dataType

    # Join with proper aliases to avoid the "trivially true" predicate
    joined = (
        d.join(
            starts,
            on=(
                (F.col("d.company_krs") == F.col("s.company_krs")) &
                (F.col("d.valid_to_entry") == F.col("s._e_from"))
            ),
            how="left"
        )
        .withColumn("_has_replacement", F.col("s._e_from").isNotNull())
    )

    # Only orphaned withdrawals become open-ended (NULL end)
    updated = joined.withColumn(
        "valid_to_entry",
        F.when(
            F.col("d.valid_to_entry").isNotNull() & (F.col("_has_replacement") == F.lit(False)),
            F.lit(None).cast(to_dtype)
        ).otherwise(F.col("d.valid_to_entry"))
    )

    # Return exactly the original columns in the original order
    return updated.select(
        *[
            (F.col("valid_to_entry") if c == "valid_to_entry" else F.col(f"d.{c}"))
            for c in df.columns
        ]
    )
