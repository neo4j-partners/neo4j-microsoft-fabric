# Integrating Data from Microsoft Fabric's Lakehouse with Neo4j Graph Database

This repository contains several methods to load data from Microsoft Fabric's Lakehouse into Neo4j using different technologies. Whether you prefer using Spark based Jupyter Notebook or Cypher queries, you'll find a solution that suits your needs.

## Method 1: Using Synapse Data Engineering Module and neo4j spark connector (load_from_data_engg_nb.ipynb)

**Description:**
- This Spark-based Python code allows you to read data from Microsoft Fabric's Lakehouse using Fabric Data Engineering module.
- You can read any set of nodes or relationships as a DataFrame in Spark, making it ideal for handling large volumes of data with high performance.

## Method 2: Using Cypher Queries (load_from_lakehouse.cypher)

**Description:**
- This method leverages Cypher, a native scripting language for Neo4j, to load data files.
- It's the simplest and most straightforward way to load data into Neo4j, making it perfect for experimental purposes.
- The script uses APOC Load JSON procedures to retrieve data from URLs or maps and transform it into map values that Cypher can consume. Cypher supports deconstructing nested documents with features like dot syntax, slices, UNWIND, and more, making it easy to turn nested data into graphs. Make sure you have the APOC library is installed in your Neo4j instance.
- Execute the Cypher query from Neo4j Browser to import data into your Neo4j database.

## Method 3: Load from GitHub (load_from_github.cypher)
**Description:**
- The Cypher query file includes identical queries as previously mentioned, but with all file URLs pointing to this GitHub repository.

## Utility: Getting an Access Token (get_access_token_and_print_cypher_stmts.sh)
- The purpose of this shell script is to obtain an access token from Azure Active Directory.
- You can use this access token as a parameter in the Cypher file above, to allow Neo4j to access Microsoft Fabric's Lakehouse content securely.

Please refer to the individual script files for detailed usage instructions. You'll find the necessary information to get started with these data loading methods.

If you have any questions or need assistance, feel free to reach out. Happy data loading!

