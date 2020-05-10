# from pyspark.sql import SparkSession
#
# def singleton(myClass):
#     instances = {}
#     def getInstance(*args, **kwargs):
#         if myClass not in instances:
#             instances[myClass] = myClass(*args, **kwargs)
#         return instances[myClass]
#     return getInstance
#
# @singleton
# class createSparkSessionTest():
#     def __init__(self):
#         print("creating spark session")
#         self.spark = \
#             SparkSession.builder.appName('PredictiveAnalysis') \
#                 .config("spark.jars", "/home/fidel/cache_pretrained/sparknlpFATjar.jar") \
#                 .master('local[*]').getOrCreate()  # sahil- fix the sparknlpJar location
#         self.spark.sparkContext.setLogLevel('ERROR')
#         print("Spark Session successfully created.")
#
#     def getSparkSession(self):
#         return self.spark
