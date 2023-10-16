# Microsoft Fabric's Lakehouse Data Loading Methods

This repository contains three different methods to load data from Microsoft Fabric's Lakehouse into Neo4j using various technologies. Whether you prefer using Spark, Cypher queries, or a shell script to get an access token, you'll find a solution that suits your needs.

## Method 1: Using Spark Jupyter Notebook (load_from_spark.ipynb)

**Description:**
- This Spark-based Python code allows you to read data from Microsoft Fabric's Lakehouse using the Bolt connector.
- You can read any set of nodes or relationships as a DataFrame in Spark, making it ideal for handling large volumes of data with high performance.

## Method 2: Using Cypher Queries (load_from_lakehouse.cypher)

**Description:**
- This method leverages Cypher, a native scripting language for Neo4j, to load data files.
- It's the simplest and most straightforward way to load data into Neo4j, making it perfect for experimental purposes.
- The script uses APOC Load JSON procedures to retrieve data from URLs or maps and transform it into map values that Cypher can consume. Cypher supports deconstructing nested documents with features like dot syntax, slices, UNWIND, and more, making it easy to turn nested data into graphs.

## Method 3: Getting an Access Token (get_access_token.sh)

**Description:**
- The purpose of this shell script is to obtain an access token from Azure Active Directory.
- You can use this access token to update the Cypher queries file, allowing secure access to Microsoft Fabric's Lakehouse.

Please refer to the individual script files for detailed usage instructions. You'll find the necessary information to get started with these data loading methods.

If you have any questions or need assistance, feel free to reach out. Happy data loading!
