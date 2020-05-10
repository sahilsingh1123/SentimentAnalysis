from viveknSentiment.viveknConstants import viveknConstants as vc
from sentiment.sentimentAnalysis.sentimentConstants import sentimentConstants as sc
from SentimentAnalysis.main.mainConstants import mainConstants as mc
from pyspark.sql.types import StringType
from viveknSentiment.viveknMain.PerformViveknSentiment import PerformViveknSentiment

class viveknStart():
    def __init__(self):
        pass

    def viveknSentiment(self, infoData):
        responseData = {}
        text = infoData.get(vc.SENTIMENTTEXT)
        spark = infoData.get(mc.SPARK)
        sentimentColName = infoData.get(sc.SENTIMENTCOLNAME)
        dataset = self.createDataframeFromText(text, spark, sentimentColName)
        infoData.update({
            mc.DATASET: dataset
        })
        responseData = PerformViveknSentiment().startAnalysis(infoData)


        """
        this is just for testing purpose we will do the actual sentiment after this...
        """
        responseData['message'] = "successfully get the responseck from the baend"

        return responseData

    def createDataframeFromText(self, text, spark, sentimentColName):
        dataset = spark.createDataFrame([text], StringType()).toDF(sentimentColName)
        return dataset
