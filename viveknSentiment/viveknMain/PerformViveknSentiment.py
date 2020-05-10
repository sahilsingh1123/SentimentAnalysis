from SentimentAnalysis.main.mainConstants import mainConstants as mc
from viveknSentiment.viveknConstants import viveknConstants as vc
from sentiment.sentimentAnalysis.sentimentConstants import sentimentConstants as sc
from sentiment.sentimentAnalysis.textPreprocessing import TextProcessing
from sparknlp.base import PipelineModel
from sparknlp.pretrained import ViveknSentimentModel
from pyspark.sql.functions import array_join
from SentimentAnalysis.main.mainUtilities import mainUtilities as mu

class PerformViveknSentiment():
    def __init__(self):
        pass

    def startAnalysis(self, infoData):
        # do words cleaning
        # call viveknPretrained MODEL,
        infoData = self.prepareData(infoData)
        infoData = self.textProcessing(infoData)
        dataset = self.vivekSentimentPretrained(infoData)
        infoData = self.mergeOriginalPredictedDataset(infoData, dataset)
        infoData = self.getSentimentValue(infoData)

        #drop the dataframe and sparkSession variable
        infoData.pop(mc.SPARK)
        infoData.pop(mc.DATASET)
        infoData.pop(mc.ORIGINALDATASET)
        return infoData


    def prepareData(self, infoData):
        dataset = infoData.get(mc.DATASET)
        originalDataset, duplicateDataset = self.createDuplicateDataset(dataset)
        infoData.update({mc.DATASET: duplicateDataset,
                         mc.ORIGINALDATASET: originalDataset})
        return infoData


    def textProcessing(self, infoData):
        dataset = self.cleanData(infoData)
        docPipeline = infoData.get(vc.EXPLAINDOCPATH)
        loadedDocPipe = PipelineModel.load(docPipeline)
        dataset = loadedDocPipe.transform(dataset)
        applySentimentOn = "lemma"
        infoData.update({
            mc.DATASET: dataset,
            vc.APPLY_SENTIMENT_ON: applySentimentOn
        })

        return infoData

    def cleanData(self, infoData):
        sentimentCol = infoData.get(sc.SENTIMENTCOLNAME)
        dataset = infoData.get(mc.DATASET)
        dataset = TextProcessing().replaceSpecialChar(dataset, sentimentCol)
        return dataset

    def vivekSentimentPretrained(self, infoData):
        applySentimentOn = infoData.get(vc.APPLY_SENTIMENT_ON)
        dataset = infoData.get(mc.DATASET)
        viveknPretrainedModelPath = infoData.get(vc.VIVEKNPRETRAINEDPATH)
        predictionCol = infoData.get(mc.PREDICTIONCOL)
        """use to download it once later we need to load it from the local to avoid dependency on online downloader."""
        viveknSentiment = ViveknSentimentModel.load(viveknPretrainedModelPath).setInputCols(
            ["document", applySentimentOn]).setOutputCol(predictionCol)
        dataset = viveknSentiment.transform(dataset)
        dataset = dataset.withColumn(predictionCol, array_join(predictionCol + ".result", ""))
        dataset = dataset.select(mc.PA_INDEX, predictionCol)
        return dataset

    def createDuplicateDataset(self, dataset):
        originalDataset, duplicateDataset = mu.duplicateDataset(dataset)
        return originalDataset, duplicateDataset

    def mergeOriginalPredictedDataset(self, infoData, dataset):
        originalDataset = infoData.get(mc.ORIGINALDATASET)
        originalDataset = mu.joinDataset(originalDataset, dataset, mc.PA_INDEX)
        infoData.update({mc.ORIGINALDATASET: originalDataset})
        return infoData

    def getSentimentValue(self, infoData):
        dataset = infoData.get(mc.ORIGINALDATASET)
        predictionCol = infoData.get(mc.PREDICTIONCOL)
        sentimentVal = dataset.select(predictionCol).first()[0]

        infoData.update({
            mc.SENTIMENTVALUE: sentimentVal
        })
        return infoData

