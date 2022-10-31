# Databricks notebook source
# MAGIC %md
# MAGIC ## Ethereum Blockchain Data Analysis - <a href=https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas>Table Schemas</a>
# MAGIC - **Transactions** - Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes. This table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.
# MAGIC - **Blocks** - The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.
# MAGIC - **Receipts** - the cost of gas for specific transactions.
# MAGIC - **Traces** - The trace module is for getting a deeper insight into transaction processing. Traces exported using <a href=https://openethereum.github.io/JSONRPC-trace-module.html>Parity trace module</a>
# MAGIC - **Tokens** - Token data including contract address and symbol information.
# MAGIC - **Token Transfers** - The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address. This table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events.
# MAGIC - **Contracts** - Some transactions create smart contracts from their input bytes, and this smart contract is stored at a particular 20-byte address. This table contains a subset of Ethereum addresses that contain contract byte-code, as well as some basic analysis of that byte-code. 
# MAGIC - **Logs** - Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. This table is generally useful for reporting on any logged event type on the Ethereum blockchain.
# MAGIC 
# MAGIC In Addition, there is a price feed that changes daily (noon) that is in the **token_prices_usd** table
# MAGIC 
# MAGIC ### Rubric for this module
# MAGIC - Transform the needed information in ethereumetl database into the silver delta table needed by your modeling module
# MAGIC - Clearly document using the notation from [lecture](https://learn-us-east-1-prod-fleet02-xythos.content.blackboardcdn.com/5fdd9eaf5f408/8720758?X-Blackboard-Expiration=1650142800000&X-Blackboard-Signature=h%2FZwerNOQMWwPxvtdvr%2FmnTtTlgRvYSRhrDqlEhPS1w%3D&X-Blackboard-Client-Id=152571&response-cache-control=private%2C%20max-age%3D21600&response-content-disposition=inline%3B%20filename%2A%3DUTF-8%27%27Delta%2520Lake%2520Hands%2520On%2520-%2520Introduction%2520Lecture%25204.pdf&response-content-type=application%2Fpdf&X-Amz-Security-Token=IQoJb3JpZ2luX2VjEAAaCXVzLWVhc3QtMSJHMEUCIQDEC48E90xPbpKjvru3nmnTlrRjfSYLpm0weWYSe6yIwwIgJb5RG3yM29XgiM%2BP1fKh%2Bi88nvYD9kJNoBNtbPHvNfAqgwQIqP%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FARACGgw2MzU1Njc5MjQxODMiDM%2BMXZJ%2BnzG25TzIYCrXAznC%2BAwJP2ee6jaZYITTq07VKW61Y%2Fn10a6V%2FntRiWEXW7LLNftH37h8L5XBsIueV4F4AhT%2Fv6FVmxJwf0KUAJ6Z1LTVpQ0HSIbvwsLHm6Ld8kB6nCm4Ea9hveD9FuaZrgqWEyJgSX7O0bKIof%2FPihEy5xp3D329FR3tpue8vfPfHId2WFTiESp0z0XCu6alefOcw5rxYtI%2Bz9s%2FaJ9OI%2BCVVQ6oBS8tqW7bA7hVbe2ILu0HLJXA2rUSJ0lCF%2B052ScNT7zSV%2FB3s%2FViRS2l1OuThecnoaAJzATzBsm7SpEKmbuJkTLNRN0JD4Y8YrzrB8Ezn%2F5elllu5OsAlg4JasuAh5pPq42BY7VSKL9VK6PxHZ5%2BPQjcoW0ccBdR%2Bvhva13cdFDzW193jAaE1fzn61KW7dKdjva%2BtFYUy6vGlvY4XwTlrUbtSoGE3Gr9cdyCnWM5RMoU0NSqwkucNS%2F6RHZzGlItKf0iPIZXT3zWdUZxhcGuX%2FIIA3DR72srAJznDKj%2FINdUZ2s8p2N2u8UMGW7PiwamRKHtE1q7KDKj0RZfHsIwRCr4ZCIGASw3iQ%2FDuGrHapdJizHvrFMvjbT4ilCquhz4FnS5oSVqpr0TZvDvlGgUGdUI4DCdvOuSBjqlAVCEvFuQakCILbJ6w8WStnBx1BDSsbowIYaGgH0RGc%2B1ukFS4op7aqVyLdK5m6ywLfoFGwtYa5G1P6f3wvVEJO3vyUV16m0QjrFSdaD3Pd49H2yB4SFVu9fgHpdarvXm06kgvX10IfwxTfmYn%2FhTMus0bpXRAswklk2fxJeWNlQF%2FqxEmgQ6j4X6Q8blSAnUD1E8h%2FBMeSz%2F5ycm7aZnkN6h0xkkqQ%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220416T150000Z&X-Amz-SignedHeaders=host&X-Amz-Expires=21600&X-Amz-Credential=ASIAZH6WM4PLXLBTPKO4%2F20220416%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=321103582bd509ccadb1ed33d679da5ca312f19bcf887b7d63fbbb03babae64c) how your pipeline is structured.
# MAGIC - Your pipeline should be immutable
# MAGIC - Use the starting date widget to limit how much of the historic data in ethereumetl database that your pipeline processes.

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address, start_date = Utils.create_widgets()
print(wallet_address, start_date)
spark.conf.set('wallet.address', wallet_address)
spark.conf.set('start.date', start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## YOUR SOLUTION STARTS HERE...

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ethereumetl;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, coalesce
from pyspark.sql.types import _parse_datatype_string, IntegerType
sqlContext.setConf('spark.sql.shuffle.partitions', 'auto')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm('/user/hive/warehouse/g01_db.db/', recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS G01_DB CASCADE;
# MAGIC 
# MAGIC CREATE SCHEMA G01_DB;

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/anthonycor/dscc202-402-spring2022_finalproject/a98d08a03070cfbc4d696f3ea55a2104e52db8a6/project4-end2end-dia/diagram_for_final.png">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Applicable Transfers (Transfers that have a USD conversion)

# COMMAND ----------

# # Grab all records that have a USD conversion & transfer occured prior to posted date
# sql_statement = """
# SELECT TT.*, USD.price_usd, TT.Value*USD.price_usd AS USDValue
#     FROM (SELECT * FROM blocks WHERE CAST((timestamp/1e6) AS TIMESTAMP) <='""" + start_date + """') AS B
#     INNER JOIN token_transfers TT ON TT.block_number = B.number
#     INNER JOIN (SELECT DISTINCT * FROM token_prices_usd WHERE asset_platform_id == 'ethereum') AS USD ON TT.token_address = USD.contract_address
# """
# erc_token_transactions = spark.sql(sql_statement)

# expected_ddl_schema = """token_address String, from_address String, to_address String, value Decimal(38,0), transaction_hash String, log_index Long, block_number Long, start_block Long, end_block Long, price_usd Double, USDValue Double"""
# assert erc_token_transactions.schema == _parse_datatype_string(expected_ddl_schema), "Incorrect Silver ERC20 Transactions schema"
# print("Assertion passed.")

# erc_token_transactions.write.mode('overwrite').option('mergeSchema', 'true').format('delta').partitionBy('start_block', 'end_block').saveAsTable('G01_db.SilverTable_ERC20Transactions')

# COMMAND ----------

# Grab all records that have a USD conversion & transfer occured prior to posted date
sql_statement = """
SELECT TT.*, USD.price_usd, TT.Value*USD.price_usd AS USDValue
    FROM token_transfers TT
    INNER JOIN (SELECT DISTINCT * FROM token_prices_usd WHERE asset_platform_id == 'ethereum') AS USD ON TT.token_address = USD.contract_address
"""
erc_token_transactions = spark.sql(sql_statement)

expected_ddl_schema = """token_address String, from_address String, to_address String, value Decimal(38,0), transaction_hash String, log_index Long, block_number Long, start_block Long, end_block Long, price_usd Double, USDValue Double"""
assert erc_token_transactions.schema == _parse_datatype_string(expected_ddl_schema), "Incorrect Silver ERC20 Transactions schema"
print("Assertion passed.")

erc_token_transactions.write.format('delta').partitionBy('start_block', 'end_block').saveAsTable('G01_db.SilverTable_ERC20Transactions')

# COMMAND ----------

spark.sql("OPTIMIZE G01_db.SilverTable_ERC20Transactions ZORDER BY (block_number)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lkp_UserWallet

# COMMAND ----------

# We use the row number to give an identity key to each wallet. We UNION the two columns to ensure we are getting ever possible address.
# We reference the previously created silver table to query from relevant transactions & transactions from posted date.
sql_statement = """
SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) as WalletID, WalletHash FROM
    (SELECT DISTINCT C AS WalletHash FROM
        (SELECT from_address as C FROM G01_db.SilverTable_ERC20Transactions) 
        UNION
        (SELECT to_address as C FROM G01_db.SilverTable_ERC20Transactions))
"""
df = spark.sql(sql_statement)
contracts_df = spark.sql(r"SELECT address FROM silver_contracts WHERE is_erc20='True'")
df = df.join(contracts_df, (df.WalletHash == contracts_df.address), 'left_anti').select('WalletID', 'WalletHash')

expected_ddl_schema = "WalletID Integer, WalletHash String"
assert df.schema == _parse_datatype_string(expected_ddl_schema), "Incorrect Silver External Wallets schema"
print("Assertion passed.")

df.write.mode('overwrite').option('mergeSchema', 'true').format('delta').saveAsTable('G01_db.SilverTable_ExternalWallets')

# COMMAND ----------

spark.sql("OPTIMIZE G01_db.SilverTable_ExternalWallets ZORDER BY (WalletID)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lkp_EthereumTokens

# COMMAND ----------

# We use the row number to give an identity key to each token. We select distinct to remove any possible duplicates. We select only ethereum tokens & capture meta data
sql_statement = """
SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS TokenID, * FROM 
    (SELECT DISTINCT contract_address, symbol, name, description, links, image, price_usd 
        FROM token_prices_usd
            WHERE asset_platform_id == 'ethereum')
"""

df = spark.sql(sql_statement)

expected_ddl_schema = "TokenID Integer, contract_address String, symbol String, name String, description String, links String, image String, price_usd Double"
assert df.schema == _parse_datatype_string(expected_ddl_schema), "Incorrect Silver Ethereum Tokens schema"
print("Assertion passed.")

df.write.mode('overwrite').option('mergeSchema', 'true').format('delta').saveAsTable('G01_db.SilverTable_EthereumTokens')

# COMMAND ----------

spark.sql("OPTIMIZE G01_db.SilverTable_EthereumTokens ZORDER BY (TokenID)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Token Balance from Genesis

# COMMAND ----------

# Strategy: Select to projects: (1) Negate the USDValue with addresses to the from address feature and (2) positive the USDValue with addresses to the to address feature
#           Then, do an outer join of these two tables, coalesce the to and from addresses to ensure we have a column with wallet address.
#           Then, do a left anti join with the contracts table to remove any contract addresses
#           Then, remove the meta information with identity ID of both meta tables
#           Drop any NULLS for safety

# It is less expensive to get Wallet ID after the computations because the inner join is expensive
sql_statement = """
SELECT from_address, token_address AS from_token_address, -SUM(USDValue) AS Total_From_Value 
    FROM G01_db.SilverTable_ERC20Transactions 
        GROUP BY from_address, token_address;
"""
from_df = spark.sql(sql_statement)

sql_statement = """
SELECT to_address, token_address AS to_token_address, SUM(USDValue) AS Total_To_Value
    FROM G01_db.SilverTable_ERC20Transactions
        GROUP BY to_address, token_address;
"""

to_df = spark.sql(sql_statement)
contracts_df = spark.sql(r"SELECT address FROM silver_contracts WHERE is_erc20='True'")

df = from_df.join(to_df, ((from_df.from_address == to_df.to_address) & (from_df.from_token_address == to_df.to_token_address)), 'full')
df = df.na.fill(0, ['Total_To_Value']).na.fill(0, ['Total_From_Value'])
df = df.withColumn('Balance', col('Total_From_Value')+col('Total_To_Value'))

df = df.withColumn('Walletaddress', coalesce(df['from_address'], df['to_address']))
df = df.withColumn('TokenAddress', coalesce(df['from_token_address'], df['to_token_address']))
df = df.drop(*('from_address', 'to_address', 'from_token_address', 'to_token_address'))
df = df.join(contracts_df, (df.Walletaddress == contracts_df.address), 'left_anti')


token_lkp_df = spark.sql('SELECT * FROM G01_db.SilverTable_EthereumTokens')
wallet_lkp_df = spark.sql('SELECT * FROM G01_db.SilverTable_ExternalWallets')

df = df.join(token_lkp_df, (df.TokenAddress == token_lkp_df.contract_address), 'inner')
df = df.join(wallet_lkp_df, (df.Walletaddress == wallet_lkp_df.WalletHash), 'inner')
df = df.drop(*('Total_From_Value', 'Total_To_Value', 'Walletaddress', 'TokenAddress', 'contract_address', 'symbol', 'name', 'description', 'links', 'image', 'price_usd', 'WalletHash', 'address'))
df = df.na.drop()
df = df.withColumn('Balance', df.Balance.cast('integer'))

assert df.schema == _parse_datatype_string("Balance Integer, TokenID Integer, WalletID Integer"), "Incorrect Silver Wallet Balance schema"
print("Assertion passed.")

df.write.mode('overwrite').option('overwriteSchema', 'true').format('delta').saveAsTable('G01_db.SilverTable_WalletBalance')

# COMMAND ----------

spark.sql("OPTIMIZE G01_db.SilverTable_WalletBalance ZORDER BY (TokenID)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Table History

# COMMAND ----------

display(DeltaTable.forPath(spark, '/user/hive/warehouse/g01_db.db/silvertable_erc20transactions/').history())

# COMMAND ----------

display(DeltaTable.forPath(spark, '/user/hive/warehouse/g01_db.db/silvertable_externalwallets/').history())

# COMMAND ----------

display(DeltaTable.forPath(spark, '/user/hive/warehouse/g01_db.db/silvertable_ethereumtokens/').history())

# COMMAND ----------

display(DeltaTable.forPath(spark, '/user/hive/warehouse/g01_db.db/silvertable_walletbalance/').history())

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))

# COMMAND ----------


