from flask import Blueprint, render_template

main = Blueprint('main', __name__)

import json
from engine import ClusteringEngine

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request


@main.route("/model1/<int:crime_id>/cluster", methods=["GET"])
def get_cluster1(crime_id):
    logger.debug("ID %s requested", crime_id)
    cluster_category = clustering_engine.get_cluster(crime_id)
    return json.dumps(cluster_category)

@main.route("/model2/<int:crime_id>/cluster", methods=["GET"])
def get_cluster2(crime_id):
    logger.debug("ID %s requested", crime_id)
    cluster_category = clustering_engine.get_cluster(crime_id)
    return json.dumps(cluster_category)

@main.route("/model3/<int:crime_id>/cluster", methods=["GET"])
def get_cluster3(crime_id):
    logger.debug("ID %s requested", crime_id)
    cluster_category = clustering_engine.get_cluster(crime_id)
    return json.dumps(cluster_category)

def create_app(spark_session, dataset_path):
    global clustering_engine

    clustering_engine = ClusteringEngine(spark_session, dataset_path)

    app = Flask(__name__)
    app.register_blueprint(main)
    return app
