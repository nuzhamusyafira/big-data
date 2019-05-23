import os
import logging
import pandas as pd
from pyspark.sql import Row
from pyspark.sql import types
from pyspark.sql.functions import explode
import pyspark.sql.functions as func
from sklearn.preprocessing import MinMaxScaler
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import DoubleType
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClusteringEngine:
    """A movie recommendation engine
    """

    def __transform_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Transforming model 1...")
        self.df_crime1 = self.df_crime1.withColumn("GEO_LAT", self.df_crime1["GEO_LAT"].cast("double"))
        self.df_crime1 = self.df_crime1.withColumn("GEO_LON", self.df_crime1["GEO_LON"].cast("double"))
        assembler = VectorAssembler(inputCols=["GEO_LAT", "GEO_LON"], outputCol='features')
        self.df_crime1 = assembler.transform(self.df_crime1)
        logger.info("Done transforming!")

        logger.info("Transforming model 2...")
        self.df_crime2 = self.df_crime2.withColumn("GEO_LAT", self.df_crime2["GEO_LAT"].cast("double"))
        self.df_crime2 = self.df_crime2.withColumn("GEO_LON", self.df_crime2["GEO_LON"].cast("double"))
        assembler = VectorAssembler(inputCols=["GEO_LAT", "GEO_LON"], outputCol='features')
        self.df_crime2 = assembler.transform(self.df_crime2)
        logger.info("Done transforming!")

        logger.info("Transforming model 3...")
        self.df_crime3 = self.df_crime3.withColumn("GEO_LAT", self.df_crime3["GEO_LAT"].cast("double"))
        self.df_crime3 = self.df_crime3.withColumn("GEO_LON", self.df_crime3["GEO_LON"].cast("double"))
        assembler = VectorAssembler(inputCols=["GEO_LAT", "GEO_LON"], outputCol='features')
        self.df_crime3 = assembler.transform(self.df_crime3)
        logger.info("Done transforming!")


    def __train_model(self):
        """Train the ALS model with the current dataset
        # """
        # logger.info("Training model 1...")
        # kmeans_1 = KMeans().setK(9).setSeed(1)
        # model_1 = kmeans_1.fit(self.df_crime1)
        # logger.info("Model 1 built!")
        # logger.info("Evaluating the model 1...")
        # self.centers_1 = model_1.clusterCenters()
        # logger.info("Model 1 Done !")

        # logger.info("Training model 2...")
        # kmeans_2 = KMeans().setK(9).setSeed(1)
        # model_2 = kmeans_2.fit(self.df_crime2)
        # logger.info("Model 2 built!")
        # logger.info("Evaluating the model 2...")
        # self.centers_2 = model_2.clusterCenters()
        # logger.info("Model 2 Done !")

        # logger.info("Training model 3...")
        # kmeans_3 = KMeans().setK(9).setSeed(1)
        # model_3 = kmeans_3.fit(self.df_crime3)
        # logger.info("Model 3 built!")
        # logger.info("Evaluating the model 3...")
        # self.centers_3 = model_3.clusterCenters()
        # logger.info("Model 3 Done !")


    def get_cluster1(self, crime_id):
        """Recommends up to movies_count top unrated movies to user_id
        """
        return 2

        
    def get_cluster2(self, crime_id):
        """Recommends up to movies_count top unrated movies to user_id
        """
        return 2

        
    def get_cluster3(self, crime_id):
        """Recommends up to movies_count top unrated movies to user_id
        """
        return 2


    def __init__(self, spark_session, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """
        logger.info("Starting up the Clustering Engine: ")
        self.spark_session = spark_session
        # Load ratings data for later use
        logger.info("Loading Crimes data...")
        file_name1 = 'model-1.txt'
        dataset_file_path1 = os.path.join(dataset_path, file_name1)
        exist = os.path.isfile(dataset_file_path1)
        if exist:
            self.df_crime1 = spark_session.read.csv(dataset_file_path1, header=None, inferSchema=True)
            self.df_crime1 = self.df_crime1.selectExpr("_c0 as INCIDENT_ID", "_c1 as GEO_LAT", "_c2 as GEO_LON")

        file_name2 = 'model-2.txt'
        dataset_file_path2 = os.path.join(dataset_path, file_name2)
        exist = os.path.isfile(dataset_file_path2)
        if exist:
            self.df_crime2 = spark_session.read.csv(dataset_file_path2, header=None, inferSchema=True)
            self.df_crime2 = self.df_crime2.selectExpr("_c0 as INCIDENT_ID", "_c1 as GEO_LAT", "_c2 as GEO_LON")

        file_name3 = 'model-3.txt'
        dataset_file_path3 = os.path.join(dataset_path, file_name3)
        exist = os.path.isfile(dataset_file_path3)
        if exist:
            self.df_crime3 = spark_session.read.csv(dataset_file_path3, header=None, inferSchema=True)
            self.df_crime3 = self.df_crime3.selectExpr("_c0 as INCIDENT_ID", "_c1 as GEO_LAT", "_c2 as GEO_LON")


        # Train the model
        self.__transform_model()
        self.__train_model()
