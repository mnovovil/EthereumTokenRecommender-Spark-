# Databricks notebook source
# MAGIC %md
# MAGIC ## Token Recommendation
# MAGIC <table border=0>
# MAGIC   <tr><td><img src='https://data-science-at-scale.s3.amazonaws.com/images/rec-application.png'></td>
# MAGIC     <td>Your application should allow a specific wallet address to be entered via a widget in your application notebook.  Each time a new wallet address is entered, a new recommendation of the top tokens for consideration should be made. <br> **Bonus** (3 points): include links to the Token contract on the blockchain or etherscan.io for further investigation.</td></tr>
# MAGIC   </table>

# COMMAND ----------

# MAGIC %run ./includes/utilities

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# Grab the global variables
wallet_address, start_date = Utils.create_widgets()
print(wallet_address, start_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your code starts here...

# COMMAND ----------

recommendations_statement = """
SELECT *
FROM goldtable_recommendations
WHERE WalletHash = '{0}'
ORDER BY rating DESC
""".format(str(wallet_address))

recommendations = spark.sql(recommendations_statement).toPandas()

if(recommendations.shape[0] == 0):
    recommendations_html = "<tr><td>Wallet address not found.</td></tr>"
else:
    recommendations_html = ""
    for index, row in recommendations.iterrows():
        recommendations_html += """
        <tr>
          <td style="padding:15px"><a href="{0}"><img src="{1}"></a></td>
          <td style="padding:15px">{2} ({3})</td>
          <td style="padding:15px">
            <a href="https://etherscan.io/address/{4}">
              <img src="https://etherscan.io/images/brandassets/etherscan-logo-circle.jpg" width="25" height="25">
            </a>
          </td>
        </tr>
        """.format(row["links"], row["image"], row["name"], row["symbol"], row["contract_address"])
    
displayHTML("""
<h2>Recommend Tokens for user address:</h2>
<p style="color:666666;font-size:1.25em">{0}</p>
<table border=0>
{1}
</table>
""".format(str(wallet_address), recommendations_html))

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
