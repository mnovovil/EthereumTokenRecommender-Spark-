# Databricks notebook source
# MAGIC %md
# MAGIC ## Rubric for this module
# MAGIC - Using the silver delta table(s) that were setup by your ETL module train and validate your token recommendation engine. Split, Fit, Score, Save
# MAGIC - Log all experiments using mlflow
# MAGIC - capture model parameters, signature, training/test metrics and artifacts
# MAGIC - Tune hyperparameters using an appropriate scaling mechanism for spark.  [Hyperopt/Spark Trials ](https://docs.databricks.com/_static/notebooks/hyperopt-spark-ml.html)
# MAGIC - Register your best model from the training run at **Staging**.

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
# MAGIC ## Your Code starts here...

# COMMAND ----------

# MAGIC %sql
# MAGIC USE G01_db;

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F
from delta.tables import *
import random

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from mlflow.models.signature import infer_signature
from mlflow.models.signature import ModelSignature
from mlflow.types.schema import Schema, ColSpec

from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# COMMAND ----------

class TokenRecommender:

    def __init__(self, data_path: str, model_name: str, min_USD_balance: int = 1, seed: int=1234)->None:
        self.data_path = data_path
        self.model_name = model_name
        self.min_USD_balance = min_USD_balance
        self.seed = seed
    
        # Create an MLflow experiment for this model
        #######################################################################################
        ## WHERE ARE WE STORING THE EXPERIMENT??? I JUST HAVE IT UNDER ME
        ########################################################################################
        experiment_directory = "/Users/" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get() + "/" + self.model_name + "-experiment/"
        mlflow.set_experiment(experiment_directory)
    
        # split the data set into train, validation and test and cache them
        # We'll hold out 60% for training, 20% of our data for validation, and leave 20% for testing
        self.raw_data = spark.read.format('delta').load(self.data_path)
        self.raw_data = self.raw_data.filter(self.raw_data.Balance >= self.min_USD_balance).cache()
       #######################################################################################
        ## WHERE ARE WE STORING THE META DATA
        ########################################################################################
        self.metadata_df = spark.read.format('delta').load(self.data_path+"metadata").cache()
        self.training_data_version = DeltaTable.forPath(spark, self.data_path+"triplets").history().head(1)[0]['version']
    
        (split_60_df, split_a_20_df, split_b_20_df) = self.raw_data.randomSplit([0.6, 0.2, 0.2], seed=self.seed)
        # Let's cache these datasets for performance
        self.training_df = split_60_df.cache()
        self.validation_df = split_a_20_df.cache()
        self.test_df = split_b_20_df.cache()
    
        # Initialize our ALS learner
        als = ALS()
        als.setMaxIter(5)\
           .setSeed(self.seed)\
           .setItemCol('TokenID')\
           .setRatingCol('Balance')\
           .setUserCol('WalletID')\
           .setColdStartStrategy('drop')

        # Now let's compute an evaluation metric for our test dataset, we Create an RMSE evaluator using the label and predicted columns
        self.reg_eval = RegressionEvaluator(predictionCol='prediction', labelCol='Balance', metricName='rmse')

        # Setup an ALS hyperparameter tuning grid search
        grid = ParamGridBuilder() \
          .addGrid(als.maxIter, [5, 10, 15]) \
          .addGrid(als.regParam, [0.15, 0.2, 0.25]) \
          .addGrid(als.rank, [4, 8, 12, 16, 20]) \
          .build()

        # Create a cross validator, using the pipeline, evaluator, and parameter grid you created in previous steps.
        self.cv = CrossValidator(estimator=als, 
                                 evaluator=self.reg_eval, 
                                 estimatorParamMaps=grid,
                                 numFolds=3)

    def train(self):
        """
        Train the ALS token recommendation using the training and validation set and the cross validation created
        at the time of instantiation.  Use MLflow to log the training results and push the best model from this
        training session to the MLflow registry at "Staging"
        """
        # Setup the schema for the model
        input_schema = Schema(
            [
                ColSpec('integer', 'new_songId'),
                ColSpec('integer', 'new_userId')
            ]
        )
        output_schema = Schema([ColSpec('double')])
        signature = ModelSignature(inputs=input_schema, 
                                   outputs=output_schema)
    
        with mlflow.start_run(run_name=self.model_name+'-run') as run:
            mlflow.set_tags({'group': '	G01', 'class': 'DSCC-402'})
            mlflow.log_params({'user_rating_training_data_version': self.training_data_version, 
                               'minimum_USD_balance':self.min_USD_balance, 
                               'seed': self.seed})
            
            # Run the cross validation on the training dataset. The cv.fit() call returns the best model it found.
            cv_model = self.cv.fit(self.training_df)
            
            # Evaluate the best model's performance on the validation dataset and log the result.
            validation_metric = self.reg_eval.evaluate(cv_model.transform(self.validation_df))
            mlflow.log_metric('test_' + self.reg_eval.getMetricName(), validation_metric) 
            
            # Log the best model.
            mlflow.spark.log_model(spark_model=cv_model.bestModel, signature=signature,
                                   artifact_path='als-model', registered_model_name=self.model_name)
        
        
        # Capture the latest model version, archive any previous Staged version, Transition this version to Staging
        client = MlflowClient()
        model_versions = []
        
        # Transition this model to staging and archive the current staging model if there is one
        for mv in client.search_model_versions(f"name='{self.modelName}'"):
            model_versions.append(dict(mv)['version'])
            if dict(mv)['current_stage'] == 'Staging':
                print("Archiving: {}".format(dict(mv)))
                
                # Archive the currently staged model
                client.transition_model_version_stage(
                    name=self.modelName,
                    version=dict(mv)['version'],
                    stage='Archived')
                
        client.transition_model_version_stage(
            name=self.modelName,
            version=model_versions[0],  # this model (current build)
            stage='Staging')

    def test(self):
        """
        Test the model in staging with the test dataset generated when this object was instantiated.
        """
        # THIS SHOULD BE THE VERSION JUST TRANINED
        model = mlflow.spark.load_model('models:/' + self.model_name + '/Staging')
        # View the predictions
        test_predictions = model.transform(self.test_df)
        RMSE = self.reg_eval.evaluate(test_predictions)
        print("Staging Model Root-mean-square error on the test dataset = " + str(RMSE))
  

    def recommend(self, WalletID: int) -> (DataFrame, DataFrame):
        """
        Method takes a specific WalletID and returns the tokens that they have listened to and a set of recommendations in rank order that they may like based on their listening history.
        """
        # Generate a dataframe of tokens that the user has held listened to
        tokens_holding = self.raw_data.filter(self.raw_data.(WalletID) == userId) \
                                                .join(self.metadata_df, 'TokenID') \
        #########################################################################
        ## HELP HERE
        #########################################################################
                                                .select('new_songId', 'artist_name', 'title','Plays')

        # Generate dataframe of unlistened tokens
        unlistened_tokens = self.raw_data.filter(~ self.raw_data['TokenID'].isin([song['TokenID'] for song in tokens_holding.collect()])) \
                                                    .select('tokens_holding').withColumn('WalletID', F.lit(WalletID)).distinct()

        # Feed unlistened tokens into model for a predicted Balance
        model = mlflow.spark.load_model('models:/'+self.model_name+'/Staging')
        predicted_listens = model.transform(unlistened_tokens)
        
        
        # HELP WITH THE RETURN
        return (tokens_holding.select('artist_name','title','Plays').orderBy('Plays', ascending = False), predicted_listens.join(self.raw_plays_df_with_int_ids, 'new_songId') \
                         .join(self.metadata_df, 'songId') \
                         .select('artist_name', 'title', 'prediction') \
                         .distinct() \
                         .orderBy('prediction', ascending = False)) 

    def recommend_for_wallets(self, num_of_tokens: int) -> DataFrame:
        """
        Generate a data frame that recommends a number of songs for each of the users in the dataset (model)
        """
        #########################################################################
        ## THIS NEEDS SOME LOVE
        #########################################################################
        model = mlflow.spark.load_model('models:/'+self.model_name+'/Staging')
        return model.stages[0].recommendForAllUsers(num_of_tokens)

# COMMAND ----------

# Return Success
dbutils.notebook.exit(json.dumps({"exit_code": "OK"}))
