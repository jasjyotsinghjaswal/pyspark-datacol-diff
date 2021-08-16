from typing import List, Tuple
import pandas as pd
from pyspark import sql
from find_datacol_diff import initialise_and_standardise_df, find_col_diff, gen_comp_col


def compute_dataframe_diff(s1: sql.DataFrame, s2: sql.DataFrame, pk_lst: List[str]) -> Tuple[
    sql.DataFrame, pd.Dataframe]:
    """

    :param s1: First Source PySpark Dataframe to Compare
    :param s2: Second Source PySpark Dataframe to Compare
    :param pk_lst : List of Strings containing Primary Key
    :return: Dataframe with n*2 + 2 columns where n is the no. of primary key columns and additional 2 columns.

             1st Column holds array of structure i.e. col name that differs for the Primary Key and Differing Values

             2nd Column is a FLAG column listing whether records are present in both sources
    """
    match_schema_df = initialise_and_standardise_df(s1=s1, s2=s2)
    s1_new, s2_new = match_schema_df["s1"], match_schema_df["s2"]
    s1_keys, s2_keys, comp_col = gen_comp_col(org_src=s1, pk_lst=pk_lst)
    diff_df, diff_cnts = find_col_diff(s1_new, s2_new, s1_keys, s2_keys, comp_col)

    return diff_df, diff_cnts
