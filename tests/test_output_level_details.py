import os

import pytest
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from find_datacol_diff import initialise_and_standardise_df, gen_comp_col, find_col_diff

# Load Environment Variables from .env
load_dotenv()

# Define input path for testing & Spark Home
dataset_pth = os.getenv("PROJ_DATASET_DIR")
os.environ["SPARK_HOME"] = os.getenv("PROJ_SPARK_HOME")

# Create Spark session and load dataframes for testing
spark = SparkSession.builder.getOrCreate()
emp100 = spark.read.option("header", True).csv(f"{dataset_pth}/employee100.csv")
emp101 = spark.read.option("header", True).csv(f"{dataset_pth}/employee101.csv")

# Call functions to rename dataframe columns with _s1,_s1 suffix and grab the mismatch stats Pandas Dataframe.
match_schema_df = initialise_and_standardise_df(s1=emp100, s2=emp101)
s1_new, s2_new = match_schema_df["s1"], match_schema_df["s2"]
s1_keys, s2_keys, comp_col = gen_comp_col(org_src=emp100, pk_lst=["id"])
diff_df, diff_cnts = find_col_diff(s1_new, s2_new, s1_keys, s2_keys, comp_col)


@pytest.mark.parametrize("colname, mismatch_count",
                         [("first_name", 83), ("last_name", 81), ("email", 81), ("ip_address", 81),
                          ("emp_join_date", 81), ("emp_country", 77), ("gender", 49)])
def test_mismatch_counts(colname, mismatch_count):
    # Test case to heck for differing counts
    computed_mismatch_count = diff_cnts[diff_cnts["ColName"] == colname]["Count"].iloc[0]
    assert computed_mismatch_count == mismatch_count
