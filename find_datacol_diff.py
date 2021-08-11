"""Importing all the necessary prerequisite for the datacol diff library"""

import sys
from typing import Dict, Union, List

import pandas as pd
import pyspark.sql.functions as fx
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, struct, array
from pyspark.sql.types import StringType
from tabulate import tabulate

import utils

# Initialise Logger
dcd = utils.create_logger("DataColDiff")


def initialise_and_standardise_df(s1: DataFrame, s2: DataFrame) -> Union[Dict[str, DataFrame], Dict[str, None]]:
    """
    Function takes 2 dataframes as input and does the following:
    a)Verifies if they have the same schema failing which returns Null as value in Dictionary for keys s1,s2
    b)Appends _s1,_s2 to the 1st & 2nd dataframe after converting all Nulls to BlankSpaces.
    c)Returns the new dataframe in a dictionary with keys s1 and s2

    :param s1: First Source Dataframe to Compare
    :param s2: Second Source Dataframe to Compare
    :return: Dictionary with the standardised dataframes
    """
    try:
        # Find difference in column
        s1_cols = set(map(str.lower, set(s1.schema.names)))
        s2_cols = set(map(str.lower, set(s2.schema.names)))
        col_diff = s1_cols - s2_cols

        # If there is no difference between column sets append _s1 and _s2 to the columns
        if not col_diff:
            # Convert Nulls to Blanks
            s1 = s1.fillna("")
            s2 = s2.fillna("")
            dcd.info(f"The schema matches.Condition Fulfilled.Columns are : {s1_cols}")
            for curr_col in s1_cols:
                s1 = s1.withColumnRenamed(curr_col, curr_col + "_s1")
                s2 = s2.withColumnRenamed(curr_col, curr_col + "_s2")

            dcd.info("Schema Renaming Complete.")

            return {"s1": s1, "s2": s2}

    except Exception as exc:
        # Handle exceptions and return None as values in the dictionary for both s1,s2
        dcd.error(utils.err_msg(lineno=sys.exc_info()[-1].tb_lineno, exception_object=exc))
        sys.exit(1)

    dcd.info("Cant Proceed.Schema for the 2 dataframes don't match")
    return {"s1": None, "s2": None}


def create_join_condition(s1: DataFrame, s2: DataFrame, pk_lst):
    """
    Takes 2 dataframes for comparison & List of Primary keys, returns the join condition and key list for the 2 sources
    :param s1: Source 1 Dataframe for comparison
    :param s2: Source 1 Dataframe for comparison
    :param pk_lst: List of Primary keys
    :return: The join condition , key list for s1 and key list for s2 in tuple
    """

    cond = [(s1[key + "_s1"] == s2[key + "_s2"]) for key in pk_lst]
    s1_key_lst = [key + "_s1" for key in pk_lst]
    s2_key_lst = [key + "_s2" for key in pk_lst]

    dcd.info(f"Join Condition : {cond} ,s1 keys : {s1_key_lst},s2 keys : {s2_key_lst}")

    return cond, s1_key_lst, s2_key_lst


def find_col_diff(s1: DataFrame, s2: DataFrame, s1_key_lst: List[str], s2_key_lst: List[str], cond: List[col]):
    # Find Original column names
    org_col = set([curr_col.split("_s1")[0] for curr_col in s1.schema.names])
    # Find Original Primary keys names
    org_pk = set([curr_col.split("_s1")[0] for curr_col in s1_key_lst])
    # Find columns to compare which are original columns except primary key
    comp_col = org_col - org_pk
    # Join the _s1,_s2 dataframes on the join condition
    s1_jn_s2 = s1.join(s2, cond, "outer").withColumn("CompColArr", fx.array())
    # Iterate all the columns and generate json with keys for differing columns
    for curr_col in comp_col:
        s1_jn_s2 = s1_jn_s2.withColumn("column_eq_test", when(
            fx.coalesce(col(curr_col + "_s1"), fx.lit("")) != fx.coalesce(col(curr_col + "_s2"), fx.lit("")),
            1).otherwise(0))
        s1_jn_s2 = s1_jn_s2.withColumn("CompColArr", when(fx.col("column_eq_test") == 1,
                                                          fx.array_union(col("CompColArr"),
                                                                         fx.array(
                                                                             struct(fx.lit(curr_col).alias("col_name"),
                                                                                    col(curr_col + "_s1").cast(
                                                                                        StringType()).alias("s1_value"),
                                                                                    col(curr_col + "_s2").cast(
                                                                                        StringType()).alias(
                                                                                        "s2_value"))))).otherwise(
            col("CompColArr")))

    # Select Primary key and Column Validation Column also making Comparison column blank for Null Keys
    comp_tbl_cols = s1_key_lst + s2_key_lst + ["CompColArr"]
    s1_jn_s2 = s1_jn_s2.select(comp_tbl_cols).withColumn("CompColArr",
                                                         when(fx.concat(*s1_key_lst).isNull(), array([])).otherwise(
                                                             col("CompColArr"))).withColumn("CompColArr",
                                                                                            when(fx.concat(
                                                                                                *s2_key_lst).isNull(),
                                                                                                 array([])).otherwise(
                                                                                                col("CompColArr"))) \
        .drop("column_eq_test")

    # Find records missing in s1 and s2
    s1_jn_s2.persist()
    only_in_s1 = s1_jn_s2.filter(fx.concat(*s2_key_lst).isNull()).select(*s1_key_lst)
    only_in_s2 = s1_jn_s2.filter(fx.concat(*s1_key_lst).isNull()).select(*s2_key_lst)
    col_diff_rec = s1_jn_s2.filter(fx.concat(*s1_key_lst).isNotNull() & fx.concat(*s2_key_lst).isNotNull())

    dcd.info("Column Comparison Complete.Find Count results below :")
    dcd.info(f"s1 : {s1.count()} , s2 : {s2.count()}")
    dcd.info(
        f"only_in_s1 : {only_in_s1.count()} , only_in_s2 : {only_in_s2.count()} , col_diff_rec : {col_diff_rec.count()} ")

    return {"s1_only": only_in_s1, "s2_only": only_in_s2, "attrib_diff": col_diff_rec}


def col_mismatch_counts(s1: DataFrame, col_diff_rec: DataFrame):
    # Find Original column names
    org_col = set([curr_col.split("_s1")[0] for curr_col in s1.schema.names])
    dict_val = {}
    for curr_col in org_col:
        curr_col_cnt = col_diff_rec.filter(fx.array_contains(fx.col("CompColArr.col_name"), curr_col)).count()
        dict_val[curr_col] = curr_col_cnt

    # Sort the Dictionary from columns having highest mismatch to lowest and create a dataframe
    return pd.DataFrame.from_dict(dict_val, orient="index", columns=["count"]).sort_values(by=["count"],
                                                                                           ascending=False)


if __name__ == "__main__":
    from pyspark.sql import SparkSession, DataFrame
    from find_datacol_diff import initialise_and_standardise_df, create_join_condition

    # Define input path for testing
    dataset_pth = "tests/datasets"

    # Create Spark session and load dataframes for testing
    spark = SparkSession.builder.getOrCreate()
    emp100 = spark.read.option("header", True).csv(f"{dataset_pth}/employee100.csv")
    emp101 = spark.read.option("header", True).csv(f"{dataset_pth}/employee101.csv")
    bible = spark.read.option("header", True).csv(f"{dataset_pth}/bible101.csv")
    match_schema_df = initialise_and_standardise_df(s1=emp100, s2=emp101)
    s1_new, s2_new = match_schema_df["s1"], match_schema_df["s2"]
    join_condition, s1_keys, s2_keys = create_join_condition(s1_new, s2_new, ["id"])
    analysed_dfs = find_col_diff(s1_new, s2_new, s1_keys, s2_keys, join_condition)
    dcd.info("Displaying Mismatch Attribute Dataframe")
    analysed_dfs["attrib_diff"].select(fx.to_json(struct("*"))).show(200, truncate=False)
    mismatch_counts_df = col_mismatch_counts(emp100,analysed_dfs["attrib_diff"])
    dcd.info("Printing Mismatch Column Count details")
    print(tabulate(mismatch_counts_df))
