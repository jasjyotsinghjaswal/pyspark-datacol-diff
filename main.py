if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import struct, to_json
    from find_datacol_diff import initialise_and_standardise_df, find_col_diff, gen_comp_col, dcd
    from dotenv import load_dotenv
    import os

    # Load Environment Variables from .env
    load_dotenv()

    # Define input path for testing
    dataset_pth = os.getenv("PROJ_DATASET_DIR")

    # Define input path for testing
    dataset_pth = "tests/datasets"

    # Create Spark session and load dataframes for testing
    spark = SparkSession.builder.getOrCreate()
    emp100 = spark.read.option("header", True).csv(f"{dataset_pth}/employee100.csv")
    emp101 = spark.read.option("header", True).csv(f"{dataset_pth}/employee101.csv")
    bible = spark.read.option("header", True).csv(f"{dataset_pth}/bible101.csv")
    match_schema_df = initialise_and_standardise_df(s1=emp100, s2=emp101)
    s1_new, s2_new = match_schema_df["s1"], match_schema_df["s2"]
    s1_keys, s2_keys, comp_col = gen_comp_col(org_src=emp100, pk_lst=["id"])
    diff_df, diff_cnts = find_col_diff(s1_new, s2_new, s1_keys, s2_keys, comp_col)
    dcd.info("Displaying Mismatch Attribute Dataframe")
    diff_df.withColumn("CompColArr", to_json(struct("CompColArr"))).show(200, truncate=False)
