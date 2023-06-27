# Project-code
Spark Code

# Databricks notebook source
spark


# COMMAND ----------
For deleting the unncessesary columns from table:
		 df2=df1.drop("Adult PD Kt/V data availability code","Adult HD Kt/V data availability code","Patient Transfusion data availability Code",)
		 df2.display()

# COMMAND ----------
For combining the address from multiple columns into single column:
		df3=df2.withColumn("Address",concat_ws(',', "Address Line 1", "Address Line 2","ZIP Code","County/Parish"))\
       		       .drop("Address Line 1",',', "Address Line 2",',',"ZIP Code",',',"County/Parish") 
		 df3.display()

# COMMAND ----------
For combining the address from multiple columns into single column:
		df3=df2.withColumn("Address",concat_ws(',', "Address Line 1", "Address Line 2","ZIP Code","County/Parish"))\
       		       .drop("Address Line 1",',', "Address Line 2",',',"ZIP Code",',',"County/Parish") 
		 df3.display()

# COMMAND ----------
For splitting the two dates from single column into two columns:
from pyspark.sql.functions import substring 
df7=df6.withColumn('Claims Start date', substring('Claims Date', 1,9))\
       .withColumn('Claims End date', substring('Claims Date', 11,9))\
       .drop('Claims Date', 'Network','Five Star Date','Patient condition after transfusion','Patient hospitalization category text','Patient Hospital Readmission Category','Patient Survival Category Text')
df7.display()


# COMMAND ----------
For coverting Profit and Non-profit (Rows data into columns)
df11 = df10.groupBy("Facility Name").pivot("Profit or Non-Profit").sum("Network")
df11.show(truncate=False)
df11.display()
