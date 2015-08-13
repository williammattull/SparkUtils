# SparkUtils
Apache Spark Utils

This is a repository for tools that I use in Apache Spark

Transpose Data - written in Python 2.7
Reads an .csv input file and creates an Key,Value pair RDD.
The RDD is grouped by key and the values are aggregated to a
single row using a comma ',' as the delimiter. The data is
then written out to a tab delimited key, values file.
