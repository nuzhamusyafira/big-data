import os
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from pyspark.sql import functions as F

class RecommendationEngine:
    """A movie recommendation engine
    """

    def __train_model(self):
        """Train the ALS model with the current dataset
        """
        logger.info("Training the ALS model...")
        self.als = ALS(maxIter=5, regParam=0.01, userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop")
        self.model = self.als.fit(self.ratings)
        logger.info("ALS model built!")

    def add_ratings(self, ratings):
        """Add additional movie ratings in the format (user_id, movie_id, rating)
        """
        # Convert ratings to a DF
        new_ratings = self.spark.createDataFrame([(user_id, movie_id, rating)], ["userId", "movieId", "rating"])
        # Add new ratings to the existing ones
        self.ratings = self.ratings.union(new_ratings)
        # Re-train the ALS model with the new ratings
        self.__train_model()
        new_ratings = new_ratings.toPandas()
        new_ratings = new_ratings.to_json()
        
        return new_ratings

    def get_ratings_for_movie_ids(self, user_id, movie_ids):
        """Given a user_id and a list of movie_ids, predict ratings for them 
        """

        requested_movies = self.spark.createDataFrame([(user_id, movie_id)], ["userId", "movieId"])
        # Get predicted ratings
        ratings = self.model.transform(requested_movies).collect()
        
        return ratings

    def get_top_ratings(self, user_id, movies_count):
        """Recommends up to movies_count top unrated movies to user_id
        """

        users = self.ratings.select(self.als.getUserCol())
        users = users.filter(users.userId == user_id)
        top_ratings = self.model.recommendForUserSubset(users, movies_count)
        top_ratings = top_ratings.select(F.col('userId'),
                                         F.col('recommendations')['movieId'].alias('movieId'))
        
        top_ratings = top_ratings.toPandas()
        top_ratings = top_ratings.to_json()
        
        return top_ratings

    def __init__(self, spark, dataset_path):
        """Init the recommendation engine given a Spark context and a dataset path
        """

        logger.info("Starting up the Recommendation Engine: ")

        self.spark = spark

        # Load ratings data for later use
        logger.info("Loading Ratings data...")
        self.ratings = self.spark.read.csv(dataset_path, header=True, inferSchema=True)
        self.ratings.createOrReplaceTempView("movies")
        self.new_id = self.spark.sql("SELECT DISTINCT userId FROM movies")
        self.new_id.createOrReplaceTempView("newId")
        self.new_user_id = self.spark.sql("SELECT userId, ROW_NUMBER() OVER (ORDER BY userId) AS user_id FROM newId")
        self.ratings = self.ratings.join(self.new_user_id, self.ratings.userId == self.new_user_id.userId)
        self.ratings = self.ratings.select('user_id', 'movieId', 'rating')
        self.ratings = self.ratings.withColumnRenamed('user_id', 'userId')

        # Train the model
        self.__train_model() 
