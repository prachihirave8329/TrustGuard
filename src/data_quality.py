
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_transactions(df: DataFrame):
    """
    Validate retail transactions.

    Returns:
        clean_df: Valid records after deduplication.
        rejected_df: Invalid records with rejection reasons.
    """

    # Identify duplicates using transaction_id.
    # Keep the first occurrence of each transaction ID.
    from pyspark.sql.window import Window

    window = Window.partitionBy("transaction_id").orderBy(
        F.col("transaction_id")
    )

    ranked_df = df.withColumn(
        "_duplicate_rank",
        F.row_number().over(window)
    )

    # Mark duplicate records, excluding missing transaction IDs.
    ranked_df = ranked_df.withColumn(
        "_is_duplicate",
        (F.col("transaction_id").isNotNull())
        & (F.trim(F.col("transaction_id")) != "")
        & (F.col("_duplicate_rank") > 1)
    )

    # Build a list of validation errors for each row.
    checked_df = ranked_df.withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("transaction_id").isNull()
                | (F.trim(F.col("transaction_id")) == ""),
                F.lit("Missing transaction ID")
            ),
            F.when(
                F.col("customer_id").isNull()
                | (F.trim(F.col("customer_id")) == ""),
                F.lit("Missing customer ID")
            ),
            F.when(
                F.col("quantity").isNull()
                | (F.col("quantity") <= 0),
                F.lit("Invalid quantity")
            ),
            F.when(
                F.col("unit_price").isNull()
                | (F.col("unit_price") < 0),
                F.lit("Invalid unit price")
            ),
            F.when(
                F.col("transaction_date").isNull()
                | F.to_date(
                    F.col("transaction_date"), "yyyy-MM-dd"
                ).isNull(),
                F.lit("Invalid transaction date")
            ),
            F.when(
                F.col("_is_duplicate"),
                F.lit("Duplicate transaction ID")
            )
        )
    )

    # Separate valid and rejected records.
    rejected_df = checked_df.filter(
        F.col("rejection_reason") != ""
    ).drop("_duplicate_rank", "_is_duplicate")

    clean_df = checked_df.filter(
        F.col("rejection_reason") == ""
    ).drop(
        "_duplicate_rank",
        "_is_duplicate",
        "rejection_reason"
    )

    return clean_df, rejected_df
