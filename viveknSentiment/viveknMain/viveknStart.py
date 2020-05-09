from viveknSentiment.viveknConstants import viveknConstants as vc
from mainConstants import mainConstants as mc
from pyspark.sql.types import StringType

class viveknStart():
    def __init__(self):
        pass

    def viveknSentiment(self, infoData):
        text = infoData.get(vc.SENTIMENTTEXT)
        spark = infoData.get(mc.SPARK)
        dataset = self.createDataframeFromText(text, spark)


        """this is just for testing purpose we will do the actual sentiment after this...
        """
        responseData = {}
        responseData['result'] = "positive"
        responseData['message'] = "successfully get the responseck from the baend"

        return responseData

    def createDataframeFromText(self, text, spark):
        dataset = spark.createDataFrame([text], StringType()).toDF("text")
        return dataset
