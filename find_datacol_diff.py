"""Importing all the necessary prerequisite for the datacol diff library"""

import sys
from typing import List, Set

import pandas as pd
import pyspark.sql.functions as fx
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, col, struct, array
from pyspark.sql.types import StringType
from tabulate import tabulate

import utils

# Initialise Logger
dcd = utils.create_logger("DataColDiff")


def initialise_and_standardise_df(s1: DataFrame, s2: DataFrame):
    """
    Function takes 2 sources as input and does the following:
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


def gen_comp_col(org_src: DataFrame, pk_lst: List):
    """
    Takes any 1 source for comparison,Primary keys,returns renamed key list for 2 sources,and comparison columns
    :param s1: Source 1 Dataframe for comparison
    :param pk_lst: List of Primary keys
    :return: The key list for s1 and s2 in tuple,and columns which will be compared against each other
    """
    try:
        # Compute key list for 2 dataframes
        s1_key_lst = [key + "_s1" for key in pk_lst]
        s2_key_lst = [key + "_s2" for key in pk_lst]
        # Find Original column names
        org_col = set(org_src.schema.names)
        # Find columns to compare which are original columns except primary key
        comp_col: Set[str] = org_col - set(pk_lst)

        dcd.info(f"s1 keys : {s1_key_lst},s2 keys : {s2_key_lst},comparision columns : {comp_col}")
    except Exception as exc:
        # Handle exceptions and return None as values in the dictionary for both s1,s2
        dcd.error(utils.err_msg(lineno=sys.exc_info()[-1].tb_lineno, exception_object=exc))
        sys.exit(1)
    return s1_key_lst, s2_key_lst, comp_col


def find_col_diff(s1_new: DataFrame, s2_new: DataFrame, s1_key_lst: List[str], s2_key_lst: List[str],
                  comp_col: Set[str]):
    """

    :param s1_new: 1st Source Dataframe with columns prefixed as _s1
    :param s2_new: 2nd Source Dataframe with columns prefixed as _s2
    :param s1_key_lst: Primary Key List for Source s1
    :param s2_key_lst: Primary Key List for Source s2
    :param comp_col: Columns to compare between 2 Dataframes
    :return: Tuple Consisting of Spark Dataframe with Mismatch Information,Pandas Dataframe with Mismatch Counts
    """
    try:
        # Compute Join Condition for 2 dataframes
        cond = [(s1_new[s1_key] == s2_new[s2_key]) for s1_key, s2_key in zip(s1_key_lst, s2_key_lst)]
        dcd.info(f"Join Condition : {cond}")
        # Join the _s1,_s2 dataframes on the join condition
        s1_jn_s2 = s1_new.join(s2_new, cond, "outer").withColumn("CompColArr", fx.array())
        # Iterate all the columns and generate json with keys for differing columns
        for curr_col in comp_col:
            # Expression to coalesce s2 and s2 column before comparison to avoid hiccups
            s1_coal_col = fx.coalesce(col(curr_col + "_s1"), fx.lit("EXVG-PYSPARK-DATACOL-DIFF"))
            s2_coal_col = fx.coalesce(col(curr_col + "_s2"), fx.lit("EXVG-PYSPARK-DATACOL-DIFF"))
            # Check equality between 2 columns.Populate 1 for mismatch,0 otherwise
            s1_jn_s2 = s1_jn_s2.withColumn("column_eq_test", when(s1_coal_col != s2_coal_col, 1).otherwise(0))
            # Create expressions for keys col_name, col_s1,col_s2 with column name and 2 columns respective values
            col_name = fx.lit(curr_col).alias("col_name")
            s1_col = col(curr_col + "_s1").cast(StringType()).alias("s1_value")
            s2_col = col(curr_col + "_s2").cast(StringType()).alias("s2_value")
            # Create array structure with appropriate keys defined above and union with existing elements of Array column
            array_structure = fx.array_union(col("CompColArr"), fx.array(struct(col_name, s1_col, s2_col)))
            # Define condition to retain original array column or new array column union-ing current differences
            equality_test_result = when(col("column_eq_test") == 1, array_structure).otherwise(col("CompColArr"))
            s1_jn_s2 = s1_jn_s2.withColumn("CompColArr", equality_test_result).drop("column_eq_test")

        # Select Primary key and Column Validation Column also making Comparison column blank for Null Keys else Retain
        comp_tbl_cols = s1_key_lst + s2_key_lst + ["CompColArr"]
        """
        The Nulls generated for unmatched records are captured as Column difference in Array column.
        The value for 1 of the dataframe's column are all captured as Null and appear as False Positive
        For such records Array Diff column is explicitly initialised as empty array
        """
        key_null_check = when(fx.concat(*s1_key_lst).isNull() | fx.concat(*s2_key_lst).isNull(), array([]))
        s1_jn_s2 = s1_jn_s2.select(comp_tbl_cols).withColumn("CompColArr", key_null_check.otherwise(col("CompColArr")))

        # Find records missing in s1 and s2 with appropriate flag column
        s1_jn_s2.persist()
        only_in_s1 = fx.concat(*s2_key_lst).isNull()
        only_in_s2 = fx.concat(*s1_key_lst).isNull()
        no_diff_rec = col("CompColArr") == array([])
        flag_col_condn = when(only_in_s1, "S1_ONLY").when(only_in_s2, "S2_ONLY").when(no_diff_rec, "NODIFF").otherwise("")
        s1_jn_s2 = s1_jn_s2.withColumn("Flag", flag_col_condn)

        # Find Counts for each kind of Flag Value
        only_in_s1_cnt = s1_jn_s2.filter(col("Flag") == "S1_ONLY").count()
        only_in_s2_cnt = s1_jn_s2.filter(col("Flag") == "S2_ONLY").count()
        no_diff_cnt = s1_jn_s2.filter(col("Flag") == "NODIFF").count()
        diff_cnt = s1_jn_s2.filter(col("Flag") == "").count()

        dcd.info("Comparision Results :")
        dcd.info(f"S1 COUNT : {s1_new.count()} , S2 COUNT : : {s2_new.count()}")
        dcd.info(f"S1 ONLY COUNT : {only_in_s1_cnt}, S2 ONLY COUNT : {only_in_s2_cnt}")
        dcd.info(f"DIFF COUNT : {diff_cnt}, NO DIFF COUNT : {no_diff_cnt} ")

        colnm_cnt = {}
        for curr_col in comp_col:
            curr_col_cnt = s1_jn_s2.filter(fx.array_contains(fx.col("CompColArr.col_name"), curr_col)).count()
            colnm_cnt[curr_col] = curr_col_cnt

        # Sort the Dictionary from columns having highest mismatch to lowest and create a dataframe
        colnm_cnt_lst = list(colnm_cnt.items())
        col_stats = pd.DataFrame(colnm_cnt_lst, columns=['ColName', 'Count']).sort_values(by='Count', ascending=False)

        dcd.info("Printing Mismatch Column Count details")
        print(tabulate(col_stats, headers=list(col_stats.columns), tablefmt='psql'))
    except Exception as exc:
        # Handle exceptions and return None as values in the dictionary for both s1,s2
        dcd.error(utils.err_msg(lineno=sys.exc_info()[-1].tb_lineno, exception_object=exc))
        sys.exit(1)
    return s1_jn_s2, col_stats
