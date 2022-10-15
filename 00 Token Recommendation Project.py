# Databricks notebook source
# MAGIC %md
# MAGIC ## Project Description
# MAGIC In this final project, you and your group will be developing an end-to-end Data-Intensive Application (DIA) that recommends ERC-20 Tokens that a given wallet address may be interested in based on their historic similarity to other wallets on the network.
# MAGIC 
# MAGIC <b>References</b>
# MAGIC - [What is Ethereum?](https://www.preethikasireddy.com/post/how-does-ethereum-work-anyway)
# MAGIC - [What is the ERC-20 Token Standard?](https://www.investopedia.com/tech/why-crypto-users-need-know-about-erc20-token-standard)
# MAGIC 
# MAGIC Consider this illustration of the application output and our process diagram.
# MAGIC <table border=0>
# MAGIC   <tr><td><h2>Application</h2></td><td><h2>Process</h2></td></tr>
# MAGIC   <tr><td><img src='https://data-science-at-scale.s3.amazonaws.com/images/rec-application.png' width=70%></td><td><img src='https://data-science-at-scale.s3.amazonaws.com/images/DIA+Framework-DIA+Process+-+1.png' width=680></td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC As a starting point for this project all groups will have access to a Dela Lake DB named ethereumetl which contains eight raw (Bronze) tables as specified by these json schemas: [Ethereum Raw Table Schemas](https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Resources and Naming Conventions1
# MAGIC 
# MAGIC - Each group has a specific Databricks Spark Cluster preconfigured for their group named **Gxx** (all provisioned the same) with 1 driver node and up to 8 workers.
# MAGIC - Starting code package is [here](https://github.com/lpalum/dscc202-402-spring2022/tree/main/project4-end2end-dia) Each group should move this starting code to their group repo to get started.
# MAGIC - Each group should create a specific MLflow experiment for all of their project runs and the model artifacts following the naming convention: **Gxx_experiment**
# MAGIC - Each group should provision a database **Gxx_db** that they should use for all of their silver and gold hive metastore tables.
# MAGIC - Each group should establish a model in the mlflow registry following the naming convention **Gxx_model** that they will use for their project.
# MAGIC 
# MAGIC **IMPORTANT**: See the configuration notebook under **includes** to set your group designation

# COMMAND ----------

# DBTITLE 0,Project Structure
# MAGIC %md
# MAGIC ## Project Structure
# MAGIC Each group is expected to divide their work among a set of notebooks within a **Databricks Repo**.  Each group should establish a group-specific GitHub repo for their project and have each group member work on their specific branch of that repository and then explicitly merge their work into the “master” project branch when appropriate. (see the class notes on how to do this).  The following illustration highlights the recommended project structure.  This approach should make it fairly straightforward to coordinate group participation and work on independent pieces of the project while having a well-identified way of integrating those pieces into a whole application that meets the requirements specified for the project.
# MAGIC 
# MAGIC ![Image](https://data-science-at-scale.s3.amazonaws.com/images/rec-block.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grading
# MAGIC **Project is Due no later than May 6th 2022**
# MAGIC <p>Each student in a given group should participate in the design and development of the application.  The group should coordinate and divide up the responsibilities needed to complete the project. Group Submission of your Data-Intensive Application should be done via blackboard by submitting the GitHub repo URL and specifying the “branch” of the repo that contains your code to be graded.
# MAGIC </p>
# MAGIC 
# MAGIC 
# MAGIC #### Points Allocation
# MAGIC - Group - Extract Tansform and Load (ETL) - 10 points
# MAGIC - Group - Exploratory Data Analysis (EDA) - 10 points
# MAGIC - Group - Modeling - 10 points
# MAGIC - Group - Monitoring - 5 points
# MAGIC - Group - Application - 5 points
# MAGIC 
# MAGIC Total of 40 points.  Good luck and have fun!
# MAGIC 
# MAGIC <b>EACH GRAD STUDENT REGISTERED FOR DSCC402 SHOULD ALSO SUBMIT AN ADDITIONAL ASSIGNMENT FOR THIS FINAL VIA BLACKBOARD</b><br>
# MAGIC Read the following paper: [Hidden Technical Debt in Machine Learning Systems](https://proceedings.neurips.cc/paper/2015/file/86df7dcfd896fcaf2674f757a2463eba-Paper.pdf)<br>
# MAGIC Answer the following questions using your project as the example:
# MAGIC - How easily can an entirely new algorithmic approach be tested at full scale?
# MAGIC - What is the transitive closure of all data dependencies?
# MAGIC - How precisely can the impact of a new change to the system be measured?
# MAGIC - Does improving one model or signal degrade others?
# MAGIC - How quickly can new members of the team be brought up to speed?

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### Application Widgets
# MAGIC - Wallet Address - text entry of the wallet address.

# COMMAND ----------

wallet_address, start_date = Utils.create_widgets()

print(wallet_address, start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ###The Extract Load and Transform (ELT)
# MAGIC - Bronze data sources of ethereum data are already in the ethereumetl hive metastore on the platform
# MAGIC - Schema validation and migration should be included in the Bronze to Silver transformation.
# MAGIC - Optimization with partitioning and Z-ordering should be appropriately employed fro all derived delta tables.
# MAGIC - A construct should be employed so any data added to Bronze data sources will be automatically ingested by your application by re-running the ELT code.
# MAGIC - ELT code should be idempotent.  No adverse effects for multiple runs.
# MAGIC - Feature engineering should be well documented.  E.g. what transformations are being employed to form the Silver data from the Bronze data?

# COMMAND ----------

# run link to the ETL notebook
result_etl = dbutils.notebook.run("01 ETL", 0, {"00.Wallet_Address":wallet_address, "01.Start_Date":start_date})

# Check for success
assert json.loads(result_etl)["exit_code"] == "OK", "ETL Failed!" # Check to see that it worked

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exploratory Data Analysis (EDA)
# MAGIC - Follow the guidelines in [Practical Advice for the analysis of large data](https://www.unofficialgoogledatascience.com/2016/10/practical-advice-for-analysis-of-large.html) on your derived tables (SILVER and GOLD)
# MAGIC - Answer the questions posed in the 02 EDA notebook

# COMMAND ----------

# run link to the EDA notebook
result_eda = dbutils.notebook.run("02 EDA", 0,  {"00.Wallet_Address":wallet_address, "01.Start_Date":start_date})

# Check for success
assert json.loads(result_eda)["exit_code"] == "OK", "EDA Failed!" # Check to see that it worked

# COMMAND ----------

# MAGIC %md
# MAGIC ###MLops Lifecycle
# MAGIC - Training model(s) at scale to recommend (rank order) tokens of interest given a specific wallet address.
# MAGIC - Register training and test data versions as well as parameters and metrics using mlflow
# MAGIC - Including model signature in the published model: Gxx_model
# MAGIC - Hyperparameter tuning at scale with mlflow comparison of performance
# MAGIC - Orchestrating workflow staging to production using clear test methods

# COMMAND ----------

# run link to the modeling notebook
result_mlops = dbutils.notebook.run("04 Modeling", 0, {"00.Wallet_Address":wallet_address, "01.Start_Date":start_date})

# Check for success
assert json.loads(result_mlops)["exit_code"] == "OK", "Modeling Failed!" # Check to see that it worked

# COMMAND ----------

# MAGIC %md
# MAGIC ###Monitoring
# MAGIC - Specify your criteria for retraining and promotion to production.
# MAGIC - Use common model performance visualizations to highlight the performance of the Staged Model vs. the Production Model.
# MAGIC - Include code that allows monitoring to automatically “promote” a model from staging to production.

# COMMAND ----------

# run link to the monitoring notebook
result_mon = dbutils.notebook.run("05 Monitoring", 0, {"00.Wallet_Address":wallet_address, "01.Start_Date":start_date})

# Check for success
assert json.loads(result_mon)["exit_code"] == "OK", "Monitoring Failed!" # Check to see that it worked

# COMMAND ----------

# MAGIC %md
# MAGIC ## Token Recommendation
# MAGIC <table border=0>
# MAGIC   <tr><td><img src='https://data-science-at-scale.s3.amazonaws.com/images/rec-application.png'></td>
# MAGIC     <td>Your application should allow a specific wallet address to be entered via a widget in your application notebook.  Each time a new wallet address is entered, a new recommendation of the top tokens for consideration should be made.  **Bonus** (3 points): include links to the Token contract on the blockchain or etherscan.io for further investigation.</td></tr>
# MAGIC   </table>  **Bonus** (3 points): include links to the Token contract on the blockchain - etherscan.io for further investigation.

# COMMAND ----------

# run link to the application notebook
result_dash = dbutils.notebook.run("06 Token Recommender", 0, {"00.Wallet_Address":wallet_address, "01.Start_Date":start_date})

# Check for success
assert json.loads(result_dash)["exit_code"] == "OK", "Token Recommendation Application Failed!" # Check to see that it worked

