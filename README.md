# pyspark-datacol-diff

PySpark utility created to quickly provide details regarding which attributes differ between 2 dataframes with same schema and primary key

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install utility.
```bash
pip3 install pyspark-datacol-diff
```

## Examples of How To Use 

Importing The Other Dependencies prior to package installation and prepare the input dataframe.

```python
from pyspark.sql import functions as fx
from pyspark.sql.functions import when, col, struct, array
from pyspark.sql import SparkSession

# Create Spark session and load dataframes for testing
spark = SparkSession.builder.getOrCreate()
SourceDF1 = spark.read.option("header", True).csv(f"{dataset_pth}/employee100.csv")
SourceDF2 = spark.read.option("header", True).csv(f"{dataset_pth}/employee101.csv")
```

Import compute_dataframe_diff function and pass the 2 dataframes to compare along with common primary keys.
```python
from pysparkdatacoldiff.find_dataframe_diff import compute_dataframe_diff
diff_df, diff_cnts = compute_dataframe_diff(s1=SourceDF1,s2=SourceDF2,pk_lst=[STRING_LIST_OF_PRIMARY_KEY_COLS])
```
You can now use the PySpark Dataframe diff_df to look into records at granular level to find which attributes they differ.

The Pandas Dataframe diff_cnts can be printed to see counts for individual attributes which differ

The sample logs generated by invoking the above function look somewhat as demonstrated in below image

![alt text](SampleLogOutput.JPG)

## Print Pyspark DataFrame to Visualise Array Json column appropriately.
This method is not required in Databricks which does a pretty-print rendering of Array-JSON columns using the display command.


```python
diff_df.withColumn("CompColArr",fx.to_json(struct("CompColArr"))).show(NUM_REC_TO_DISPLAY, truncate=False)
```
*NUM_REC_TO_DISPLAY* = Number of Records you would want to display.Appropriately use a filter for specific records using Primary Key Filter to limit records displayed on console.

## See Records with mismatch for specific column
Use the below filter expression on CompColArr to get records that have mismatch for a particular column.Filter for fewer results.

```python
diff_df.filter(fx.array_contains(fx.col("CompColArr.col_name"), YOUR_DESIRED_COLUMN)).show(NUM_REC_TO_DISPLAY, truncate=False)
```
*NUM_REC_TO_DISPLAY* = Number of Records you would want to display.Appropriately use a filter for specific records using Primary Key Filter to limit records displayed on console.

*YOUR_DESIRED_COLUMN* = Column Name whose mismatches you want to display.

## Watch Below Image Or Follow the Collab Link on how to setup and use this utility

https://colab.research.google.com/drive/1HwX3UF5FmzpPMUjuBtnoU-VLTJ1xnWJw?usp=sharing#scrollTo=Rvd1ka4PjQHB

![alt text](CollabNotebookPySparkDataColDiffExample.png)
