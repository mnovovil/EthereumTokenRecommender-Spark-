-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Ethereum Blockchain Data Analysis - <a href=https://github.com/blockchain-etl/ethereum-etl-airflow/tree/master/dags/resources/stages/raw/schemas>Table Schemas</a>
-- MAGIC - **Transactions** - Each block in the blockchain is composed of zero or more transactions. Each transaction has a source address, a target address, an amount of Ether transferred, and an array of input bytes. This table contains a set of all transactions from all blocks, and contains a block identifier to get associated block-specific information associated with each transaction.
-- MAGIC - **Blocks** - The Ethereum blockchain is composed of a series of blocks. This table contains a set of all blocks in the blockchain and their attributes.
-- MAGIC - **Receipts** - the cost of gas for specific transactions.
-- MAGIC - **Traces** - The trace module is for getting a deeper insight into transaction processing. Traces exported using <a href=https://openethereum.github.io/JSONRPC-trace-module.html>Parity trace module</a>
-- MAGIC - **Tokens** - Token data including contract address and symbol information.
-- MAGIC - **Token Transfers** - The most popular type of transaction on the Ethereum blockchain invokes a contract of type ERC20 to perform a transfer operation, moving some number of tokens from one 20-byte address to another 20-byte address. This table contains the subset of those transactions and has further processed and denormalized the data to make it easier to consume for analysis of token transfer events.
-- MAGIC - **Contracts** - Some transactions create smart contracts from their input bytes, and this smart contract is stored at a particular 20-byte address. This table contains a subset of Ethereum addresses that contain contract byte-code, as well as some basic analysis of that byte-code. 
-- MAGIC - **Logs** - Similar to the token_transfers table, the logs table contains data for smart contract events. However, it contains all log data, not only ERC20 token transfers. This table is generally useful for reporting on any logged event type on the Ethereum blockchain.
-- MAGIC 
-- MAGIC ### Rubric for this module
-- MAGIC Answer the quetions listed below.

-- COMMAND ----------

-- MAGIC %run ./includes/utilities

-- COMMAND ----------

-- MAGIC %run ./includes/configuration

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Grab the global variables
-- MAGIC wallet_address, start_date = Utils.create_widgets()
-- MAGIC print(wallet_address, start_date)
-- MAGIC spark.conf.set('wallet.address', wallet_address)
-- MAGIC spark.conf.set('start.date', start_date)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC USE ethereumetl;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the maximum block number and date of block in the database
-- MAGIC 
-- MAGIC ## A: The max block number is 14044000 on 2022-01-20

-- COMMAND ----------

SELECT 
  number, CAST(timestamp AS TIMESTAMP) 
FROM blocks 
WHERE number IN (SELECT MAX(number) FROM blocks)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: At what block did the first ERC20 transfer happen?
-- MAGIC 
-- MAGIC ## A: 913198

-- COMMAND ----------

SELECT 
  MIN(TT.block_number)
FROM token_transfers TT
INNER JOIN silver_contracts SC ON SC.address = TT.token_address
WHERE SC.is_erc20 = "True"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: How many ERC20 compatible contracts are there on the blockchain?
-- MAGIC 
-- MAGIC ## A: 181937

-- COMMAND ----------

SELECT 
  COUNT(DISTINCT address) 
FROM silver_contracts 
WHERE is_erc20 = "True"

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Q: What percentage of transactions are calls to contracts
-- MAGIC 
-- MAGIC ## A: 45.7%

-- COMMAND ----------

SELECT
  SUM(CAST((C.address IS NOT NULL) AS INTEGER)) as to_contract,
  COUNT(1) as total_transactions,
  SUM(CAST((C.address IS NOT NULL) AS INTEGER))/COUNT(1) as percentage
FROM transactions T
LEFT JOIN contracts C ON C.address = T.to_address

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What are the top 100 tokens based on transfer count?

-- COMMAND ----------

SELECT
  token_address, COUNT(transaction_hash) transaction_count
FROM token_transfers
GROUP BY token_address
ORDER BY transaction_count DESC
LIMIT 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What fraction of ERC-20 transfers are sent to new addresses
-- MAGIC (i.e. addresses that have a transfer count of 1 meaning there are no other transfers to this address for this token this is the first)
-- MAGIC 
-- MAGIC ## A: 76.4%

-- COMMAND ----------

-- Refactor Stefano's answer to be a fraction not just the 
-- individual rows where transaction count to address is 1
SELECT
  SUM(CAST((transaction_count = 1) AS INTEGER)) as single_transfers,
  COUNT(1) as total_transfers,
  SUM(CAST((transaction_count = 1) AS INTEGER))/COUNT(1) as percentage
FROM (
  SELECT 
    TT.token_address, TT.to_address, COUNT(TT.transaction_hash) as transaction_count
  FROM token_transfers TT
  INNER JOIN silver_contracts SC ON SC.address = TT.token_address
  WHERE SC.is_erc20 = 'True'
  GROUP BY token_address, to_address
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: In what order are transactions included in a block in relation to their gas price? 7
-- MAGIC - hint: find a block with multiple transactions 
-- MAGIC 
-- MAGIC ## A: The order of the transaction included in a block are in gas price descending order.

-- COMMAND ----------

/*
-- Find a block number with more than 1 transaction in the
-- last partition of block table
SELECT number, transaction_count
FROM blocks
WHERE start_block>=14030000 and transaction_count > 1
LIMIT 10
*/

-- List all 155 transactions in this specific block
-- The transactions look to be ordered with gas price in decending order
SELECT 
  hash, block_number, transaction_index, gas_price 
FROM transactions 
WHERE start_block>=14030000 and block_number = 14030401

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What was the highest transaction throughput in transactions per second?
-- MAGIC hint: assume 15 second block time
-- MAGIC 
-- MAGIC ## A: 95.4 transactions per second

-- COMMAND ----------

SELECT MAX(transaction_count)/15 as max_tps
FROM blocks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the total Ether volume?
-- MAGIC Note: 1x10^18 wei to 1 eth and value in the transaction table is in wei

-- COMMAND ----------

SELECT sum(value)/1000000000000000000 as ether_volume
FROM transactions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: What is the total gas used in all transactions?
-- MAGIC 
-- MAGIC ## A: 93783326139907

-- COMMAND ----------

-- Total gas used in all transactions = 93783326139907
-- gas_used = "The amount of gas used by this specific transaction alone"
SELECT SUM(gas_used)
FROM Receipts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: Maximum ERC-20 transfers in a single transaction
-- MAGIC 
-- MAGIC ## A: The max ERC-20 transfers in a single block is 3016

-- COMMAND ----------

SELECT 
  block_number, COUNT(transaction_hash) transfer_count
FROM token_transfers TT
INNER JOIN silver_contracts SC ON SC.address = TT.token_address
WHERE SC.is_erc20 = 'True'
GROUP BY block_number
ORDER BY COUNT(transaction_hash) DESC
LIMIT 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Q: Token balance for any address on any date?

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Anthony's attempt to INNER JOIN token_transfers and blocks
-- MAGIC ## and sum values for each token_address for transfers on or before
-- MAGIC ## date for given wallet address
-- MAGIC sqlContext.setConf('spark.sql.shuffle.partitions', 'auto')
-- MAGIC  
-- MAGIC sql_statement = """
-- MAGIC SELECT
-- MAGIC   token_address, 
-- MAGIC   SUM(
-- MAGIC     CASE
-- MAGIC       WHEN from_address = '{wallet_address}' THEN -1*value
-- MAGIC       ELSE value
-- MAGIC     END
-- MAGIC    ) as value
-- MAGIC FROM token_transfers T
-- MAGIC INNER JOIN blocks B ON 
-- MAGIC   B.start_block = T.start_block AND 
-- MAGIC   B.end_block = T.end_block AND 
-- MAGIC   B.number = T.block_number AND
-- MAGIC   to_date(CAST(B.timestamp as TIMESTAMP)) <= '{asof_date}' AND 
-- MAGIC   (from_address = '{wallet_address}' OR to_address = '{wallet_address}')
-- MAGIC GROUP BY token_address
-- MAGIC """.format(
-- MAGIC     wallet_address = spark.conf.get('wallet.address'), 
-- MAGIC     asof_date = spark.conf.get('start.date')
-- MAGIC )
-- MAGIC 
-- MAGIC display(spark.sql(sql_statement))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viz the transaction count over time (network use)

-- COMMAND ----------

-- Refactor Brooke's answer into a single query
SELECT 
  to_date(CAST(timestamp as TIMESTAMP)) as date,
  SUM(transaction_count) as transaction_count
FROM blocks
GROUP BY to_date(CAST(timestamp as TIMESTAMP))
ORDER BY to_date(CAST(timestamp as TIMESTAMP))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Viz ERC-20 transfer count over time
-- MAGIC interesting note: https://blog.ins.world/insp-ins-promo-token-mixup-clarified-d67ef20876a3

-- COMMAND ----------

-- Ugly double aggregation sub-queries...
-- inner most query groups token_transfers by block
-- then joins blocks to get date
-- final, outer, aggregation by date to sum total token transfers (in all blocks on those dates)
SELECT
  block_date as transfer_date,
  SUM(num_transactions) total_transfers
FROM (
  -- This sub-query gets the date associated with the token transfers' block
  SELECT
    block_number, 
    num_transactions,
    to_date(CAST(timestamp AS TIMESTAMP)) as block_date
  FROM (
    -- This sub-query counts token transfers by block
    SELECT
      block_number,
      COUNT(transaction_hash) num_transactions,
      start_block, 
      end_block
    FROM token_transfers TT
    INNER JOIN silver_contracts SC ON SC.address = TT.token_address
    WHERE SC.is_erc20 = 'True'
    GROUP BY block_number, start_block, end_block
  ) TT
  LEFT JOIN blocks B ON B.number = TT.block_number AND B.start_block >= TT.start_block AND B.end_block <= TT.end_block
)
GROUP BY block_date
ORDER BY block_date ASC 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Return Success
-- MAGIC dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
