# pyspark Exercise

This document is designed to be read in parallel with the code in the pyspark_exercise_Colruyt repository. This project addresses the following topics:
    1) Set up Instruction
    2) Code deatils


Set up Instruction :- 
  Used Google colab spark setup to install spark & java image by the blow command ->
   !apt-get install openjdk-11-jdk-headless -qq > /dev/null
   !wget -q https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
   !tar xf spark-3.4.1-bin-hadoop3.tgz
   !pip install -q findspark

 once the installation done, environment variable defined ->
    environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
    environ["SPARK_HOME"] = "/content/spark-3.4.1-bin-hadoop3"

Code Details :-

  1. Once the set up is done, we have create a spark seession by the below import command 
    'from pyspark.sql import SparkSession'
  2. Created a logger object that logs to a file "assignment.log"
  3. Used the download_data_from_api function to download data of the brands from API and save as JSON files
  4. Used get_data_by_brand function to read the file and created a dataframe.
  5. Then fetch the data from API endpoints for all brands one by one and process into DataFrames
  6. As different brands might have different schema, merged_schema function is to create a unified schema that encompasses all possible fields from the JSON files retrieved from different API endpoints.
  7. We have use adjust_schema function in the provided code is to ensure that each DataFrame has a consistent set of columns based on a specified list of common columns.
  8. Created the dataframe by using adjust schema.


