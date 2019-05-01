from flask import Blueprint
main = Blueprint('main', __name__)
 
import json
from engine import RecommendationEngine
 
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
 
from flask import Flask, request
 
@main.route("/<int:user_id>/ratings/top/<int:count>", methods=["GET"])
def top_ratings(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id,count)
    return json.dumps(top_ratings)

@main.route("/<int:user_id>/ratings/<int:movie_id>", methods=["GET"])
def movie_ratings(user_id, movie_id):
    logger.debug("User %s rating requested for movie %s", user_id, movie_id)
    ratings = recommendation_engine.get_ratings_for_movie_ids(user_id, [movie_id])
	return json.dumps(ratings)

@main.route("/<int:user_id>/ratings", methods = ["POST"])
def add_ratings(user_id):
    # get the ratings from the Flask POST request object
    movie_id = int(request.form.get('movie_id'))
    rating = float(request.form.get('rating'))
    new_rating = recommendation_engine.add_ratings(user_id, movie_id, rating)
    
    return json.dumps(new_rating)

def create_app(spark, dataset_path):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(spark, dataset_path)    
    
    app = Flask(__name__)
    app.debug = True
    app.register_blueprint(main)
    return app 
