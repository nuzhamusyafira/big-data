import cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext

import findspark
findspark.init()

from pyspark.sql import SparkSession

def init_spark():
    # load spark
    session = SparkSession.builder.appName("Python Spark Recommendation Systems Example").getOrCreate()
    # IMPORTANT: pass aditional Python modules to each worker
    app_py = os.path.join(os.getcwd(), "app.py")
    engine_py = os.path.join(os.getcwd(), "engine.py")
    session.sparkContext.addPyFile(engine_py)
    session.sparkContext.addPyFile(app_py)
     
    return session
 
 
def run_server(app):
 
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)
 
    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')
 
    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 6000,
        'server.socket_host': '127.0.0.1'
    })
 
    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
 
 
if __name__ == "__main__":
    # Init spark and load libraries
    spark = init_spark()
    dataset_path = "dataset/item_ratings.csv"
    app = create_app(spark, dataset_path)
 
    # start web server
    run_server(app)

