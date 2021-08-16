from typing import List, Tuple
import pandas as pd
from pyspark import sql
from find_datacol_diff import initialise_and_standardise_df, find_col_diff, gen_comp_col


def compute_dataframe_diff(s1: sql.DataFrame, s2: sql.DataFrame, pk_lst: List[str]):
    """
    Function that takes in 2 sources to compare and the primary key and returns Tuple of Pyspark Dataframe and Pandas Dataframe

    :param s1: First Source PySpark Dataframe to Compare
    :param s2: Second Source PySpark Dataframe to Compare
    :param pk_lst: List of Strings containing Primary Key

    :return: Tuple Consisting of Spark Dataframe with Mismatch Information,Pandas Dataframe with Mismatch Counts.Spark Dataframe consists of  n*2 + 2 columns.The 1st n*2 columns are primary key columns returned from both the sources.Next Column holds array of structure i.e. col name that differs for the Primary Key and Differing Values.Next Column is a FLAG column listing whether records are present in both sources.Pandas Dataframe consists of Column Name and Record Counts for which they are different.

    """
    match_schema_df = initialise_and_standardise_df(s1=s1, s2=s2)
    s1_new, s2_new = match_schema_df["s1"], match_schema_df["s2"]
    s1_keys, s2_keys, comp_col = gen_comp_col(org_src=s1, pk_lst=pk_lst)
    diff_df, diff_cnts = find_col_diff(s1_new, s2_new, s1_keys, s2_keys, comp_col)

    return diff_df, diff_cnts
