from pyspark.sql import SparkSession
class CreateSparkSession(object):
    _instance = None
    def __new__(self):
        if not self._instance:
            print("Creating spark session...")
            self._instance = super(CreateSparkSession, self).__new__(self)
            self.spark = \
                SparkSession.builder.appName('Analysis') \
                    .config("spark.jars", "/home/fidel/cache_pretrained/sparknlpFATjar.jar") \
                    .master('local[*]').getOrCreate()  # sahil- fix the sparknlpJar location
            self.spark.sparkContext.setLogLevel('ERROR')
            print("Spark Session successfully created.")
        return self._instance

    @classmethod
    def getInstance(cls):
        if cls._instance is None:
            cls._instance = CreateSparkSession()
        return cls._instance

    def getSparkSession(self):
        return self.spark
