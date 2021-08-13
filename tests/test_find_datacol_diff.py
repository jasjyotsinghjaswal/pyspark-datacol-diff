"""
Import dependencies for the test
"""
import pyspark.sql
from pyspark.sql import SparkSession, DataFrame
from find_datacol_diff import initialise_and_standardise_df, gen_comp_col
import os
from dotenv import load_dotenv

# Load Environment Variables from .env
load_dotenv()

# Define input path for testing & Spark Home
dataset_pth = os.getenv("PROJ_DATASET_DIR")
os.environ["SPARK_HOME"] = os.getenv("PROJ_SPARK_HOME")

# Create Spark session and load dataframes for testing
spark = SparkSession.builder.getOrCreate()
emp100 = spark.read.option("header", True).csv(f"{dataset_pth}/employee100.csv")
emp101 = spark.read.option("header", True).csv(f"{dataset_pth}/employee101.csv")
bible = spark.read.option("header", True).csv(f"{dataset_pth}/bible101.csv")


def test_matching_files():
    # Checking that pair of files with same schema should always return Dataframe type in Value of Dictionary
    match_schema_df = initialise_and_standardise_df(s1=emp100, s2=emp101)
    s1_new = match_schema_df["s1"]
    s2_new = match_schema_df["s2"]

    assert type(s1_new) == DataFrame and type(s2_new) == DataFrame, "Dataframe not returned for matching Schema"


def test_matching_files_schema():
    # Checking if for matched files _s1 gets appended to s1 and _s2 to s2 dataframe
    match_schema_df = initialise_and_standardise_df(s1=emp100, s2=emp101)
    s1_cols = list([True if "_s1" in curr_col else False for curr_col in match_schema_df["s1"].schema.names])
    s2_cols = list([True if "_s2" in curr_col else False for curr_col in match_schema_df["s2"].schema.names])
    s1_bool = False if False in s1_cols else True
    s2_bool = False if False in s2_cols else True
    assert s1_bool and s2_bool, "Dataframe Cols for either s1 or s2 not ending with their respective prefix"


def test_unmatched_files():
    # Checking that pair of files with same schema should always return None type in Value of Dictionary
    unequal_schema_df = initialise_and_standardise_df(s1=emp100, s2=bible)
    assert unequal_schema_df["s1"] is None and unequal_schema_df["s2"] is None, "None not returned for Unequal Schema"


def test_join_keys_and_comp_cols():
    # Checking if generated key list for 2 sources is correct
    s1_keys, s2_keys, comp_cols = gen_comp_col(emp100, ["id", "gender"])
    id_check = True if "id_s1" in s1_keys and "id_s2" in s2_keys else False
    gender_check = True if "gender_s1" in s1_keys and "gender_s2" in s2_keys else False
    # Checking if Comparison Column should have everything except id and gender
    expected_comp_cols = {"emp_country", "last_name", "first_name", "emp_join_date", "email", "ip_address"}
    comp_col_check = comp_cols == expected_comp_cols
    assert id_check and gender_check and len(s1_keys) == 2 and len(s2_keys) == 2 and comp_col_check
