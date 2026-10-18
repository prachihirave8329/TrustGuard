
import csv
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_quality import validate_transactions


# --------------------------------------------------
# 1. Project paths
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_FILE = PROJECT_ROOT / "data" / "raw" / "retail_transactions.csv"
CLEAN_FILE = PROJECT_ROOT / "data" / "clean" / "transactions.csv"
REJECTED_FILE = PROJECT_ROOT / "data" / "rejected" / "transactions.csv"
REPORT_FILE = PROJECT_ROOT / "reports" / "quality_summary.csv"


# --------------------------------------------------
# 2. Save a Spark DataFrame as a CSV file
# --------------------------------------------------

def save_dataframe_as_csv(df, output_file):
    """Save a Spark DataFrame to a single local CSV file."""

    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open(
        mode="w",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.writer(file)

        # Write column headers
        writer.writerow(df.columns)

        # Write records
        for row in df.toLocalIterator():
            writer.writerow([
                "" if value is None else value
                for value in row
            ])

    print(f"Saved successfully: {output_file}")


# --------------------------------------------------
# 3. Save the quality summary using Python CSV
# --------------------------------------------------

def save_quality_summary(total_count, clean_count, rejected_count):
    """Save summary metrics without using a Spark DataFrame."""

    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with REPORT_FILE.open(
        mode="w",
        newline="",
        encoding="utf-8"
    ) as file:
        writer = csv.writer(file)

        writer.writerow(["metric", "value"])
        writer.writerows([
            ("raw_records", total_count),
            ("clean_records", clean_count),
            ("rejected_records", rejected_count),
        ])

    print(f"Saved successfully: {REPORT_FILE}")


# --------------------------------------------------
# 4. Main pipeline
# --------------------------------------------------

def main():

    spark = (
        SparkSession.builder
        .appName("TrustGuardDataQualityPipeline")
        .master("local[*]")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:

        # Step 1: Check the input file
        if not RAW_FILE.exists():
            raise FileNotFoundError(
                f"Input CSV file not found: {RAW_FILE}"
            )

        # Step 2: Read raw CSV data
        raw_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .csv(str(RAW_FILE))
        )

        total_count = raw_df.count()

        print("\n========== TRUSTGUARD PIPELINE ==========")
        print(f"Raw records: {total_count}")

        # Step 3: Safely convert data types
        typed_df = (
            raw_df
            .withColumn(
                "quantity",
                F.expr("try_cast(quantity AS INT)")
            )
            .withColumn(
                "unit_price",
                F.expr("try_cast(unit_price AS DOUBLE)")
            )
            .withColumn(
                "transaction_date",
                F.when(
                    F.expr(
                        "try_to_date("
                        "transaction_date, 'yyyy-MM-dd'"
                        ")"
                    ).isNotNull(),
                    F.col("transaction_date")
                ).otherwise(
                    F.lit(None).cast("string")
                )
            )
        )

        # Step 4: Validate records
        clean_df, rejected_df = validate_transactions(typed_df)

        clean_count = clean_df.count()
        rejected_count = rejected_df.count()

        print(f"Clean records: {clean_count}")
        print(f"Rejected records: {rejected_count}")

        # Step 5: Reconcile record counts
        if clean_count + rejected_count != total_count:
            raise RuntimeError(
                "Record count mismatch! "
                f"Raw={total_count}, "
                f"Clean={clean_count}, "
                f"Rejected={rejected_count}"
            )

        print("Record count reconciliation: PASSED")

        # Step 6: Save clean records
        save_dataframe_as_csv(clean_df, CLEAN_FILE)

        # Step 7: Save rejected records
        save_dataframe_as_csv(rejected_df, REJECTED_FILE)

        # Step 8: Save quality summary
        save_quality_summary(
            total_count,
            clean_count,
            rejected_count
        )

        # Step 9: Display rejected records
        if rejected_count > 0:
            print("\n========== REJECTED RECORDS ==========")

            rejected_df.select(
                "transaction_id",
                "customer_id",
                "quantity",
                "unit_price",
                "transaction_date",
                "rejection_reason"
            ).show(20, truncate=False)

        # Step 10: Final summary
        print("\n========== PIPELINE SUMMARY ==========")
        print(f"Input records     : {total_count}")
        print(f"Valid records     : {clean_count}")
        print(f"Rejected records  : {rejected_count}")
        print("Reconciliation    : PASSED")
        print("Pipeline status   : SUCCESS")

    finally:
        spark.stop()


# --------------------------------------------------
# 5. Entry point
# --------------------------------------------------

if __name__ == "__main__":
    main()
