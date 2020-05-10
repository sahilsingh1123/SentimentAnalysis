import json
import re
# from scipy.stats import norm
# import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import abs as absSpark, sqrt as sqrtSpark, mean as meanSpark, stddev as stddevSpark, lit
from pyspark.sql.types import *
# from PredictionAlgorithms.PredictiveDataTransformation import PredictiveDataTransformation
# from PredictionAlgorithms.PredictiveConstants import PredictiveConstants as mc
from SentimentAnalysis.main.mainConstants import mainConstants as mc
from pyspark.ml.feature import CountVectorizer, VectorAssembler, StringIndexer, StringIndexerModel, IndexToString

class mainUtilities():

    @staticmethod
    def summaryTable(featuresName, featuresStat):
        statDict = {}
        for name, stat in zip(featuresName.values(),
                              featuresStat.values()):
            statDict[name] = stat
        return statDict

    @staticmethod
    def writeToParquet(fileName, locationAddress, userId, data):
        extention = ".parquet"
        fileName = fileName.upper()
        userId = "" if (userId == "" or userId == None) else userId.upper()
        fileNameWithPath = locationAddress + userId + fileName + extention
        data.write.parquet(fileNameWithPath, mode="overwrite")
        onlyFileName = userId + fileName
        result = {"fileNameWithPath": fileNameWithPath,
                  "onlyFileName": onlyFileName}
        return result

    @staticmethod
    def scaleLocationGraph(label, predictionTargetData, residualsData, modelSheetName, spark):
        sparkContext = spark.sparkContext
        schema = StructType(
            [StructField('stdResiduals', DoubleType(), True), StructField(modelSheetName, DoubleType(), True)])
        try:
            predictionTrainingWithTarget = \
                predictionTargetData.select(label, modelSheetName,
                                            sqrtSpark(absSpark(predictionTargetData[label])).alias("sqrtLabel"))

            predictionTrainingWithTargetIndexing = \
                predictionTrainingWithTarget.withColumn(mc.ROW_INDEX,
                                                        F.monotonically_increasing_id())
            residualsTrainingIndexing = \
                residualsData.withColumn(mc.ROW_INDEX,
                                         F.monotonically_increasing_id())
            residualsPredictiveLabelDataTraining = \
                predictionTrainingWithTargetIndexing.join(residualsTrainingIndexing,
                                                          on=[mc.ROW_INDEX]).sort(
                    mc.ROW_INDEX).drop(mc.ROW_INDEX)
            stdResiduals = \
                residualsPredictiveLabelDataTraining.select("sqrtLabel", modelSheetName,
                                                            (residualsPredictiveLabelDataTraining["residuals"] /
                                                             residualsPredictiveLabelDataTraining[
                                                                 "sqrtLabel"]).alias("stdResiduals"))
            sqrtStdResiduals = \
                stdResiduals.select("stdResiduals", modelSheetName,
                                    sqrtSpark(absSpark(stdResiduals["stdResiduals"])).alias(
                                        "sqrtStdResiduals"))
            sqrtStdResiduals = sqrtStdResiduals.select("stdResiduals", modelSheetName)
            sqrtStdResiduals.na.drop()
            print("scaleLocation plot : success")
        except:
            sqrtStdResiduals = spark.createDataFrame(sparkContext.emptyRDD(), schema)
            print("scaleLocation plot : failed")


        return sqrtStdResiduals

    @staticmethod
    def residualsFittedGraph(residualsData, predictionData, modelSheetName, spark):
        sparkContext = spark.sparkContext
        schema = StructType(
            [StructField(modelSheetName, DoubleType(), True), StructField('residuals', DoubleType(), True)])

        try:
            predictionData = predictionData.select(modelSheetName)
            residualsTrainingIndexing = residualsData.withColumn(mc.ROW_INDEX,
                                                                 F.monotonically_increasing_id())
            predictionTrainingIndexing = predictionData.withColumn(mc.ROW_INDEX,
                                                                   F.monotonically_increasing_id())
            residualsPredictiveDataTraining = \
                predictionTrainingIndexing.join(residualsTrainingIndexing,
                                                on=[mc.ROW_INDEX]).sort(
                    mc.ROW_INDEX).drop(mc.ROW_INDEX)
            residualsPredictiveDataTraining.na.drop()
            print("residual fitted plot : success")
        except:
            residualsPredictiveDataTraining = spark.createDataframe(sparkContext.emptyRDD(), schema)
            print("residual fitted plot : failed")

        return residualsPredictiveDataTraining

    # @staticmethod
    # def quantileQuantileGraph(residualsData, spark):
    #     sparkContext = spark.sparkContext
    #     schema = StructType(
    #         [StructField('theoryQuantile', DoubleType(), True), StructField('practicalQuantile', DoubleType(), True)])
    # 
    #     try:
    #         sortedResiduals = residualsData.sort("residuals")
    #         residualsCount = sortedResiduals.count()
    #         quantile = []
    #         for value in range(0, residualsCount):
    #             quantile.append((value - 0.5) / residualsCount)
    #         zTheory = []
    #         for value in quantile:
    #             zTheory.append(norm.ppf(abs(value)))
    # 
    #         meanStdDev = []
    #         stat = \
    #             sortedResiduals.select(meanSpark("residuals"), stddevSpark("residuals"))
    #         for rows in stat.rdd.toLocalIterator():
    #             for row in rows:
    #                 meanStdDev.append(row)
    #         meanResiduals = meanStdDev[0]
    #         stdDevResiduals = meanStdDev[1]
    #         zPractical = []
    #         for rows in sortedResiduals.rdd.toLocalIterator():
    #             for row in rows:
    #                 zPractical.append((row - meanResiduals) / stdDevResiduals)
    #         quantileTheoryPractical = []
    #         for theory, practical in zip(zTheory, zPractical):
    #             quantileTheoryPractical.append([round(theory, 5),
    #                                             round(practical, 5)])
    #         '''
    #         #for future
    #         schemaQuantile=StructType([StructField("theoryQuantile",DoubleType(),True),
    #                                    StructField("practicalQuantile",DoubleType(),True)])
    #         quantileDataframe=spark.createDataFrame(quantileTheoryPractical,schema=schemaQuantile)
    #         '''
    #         quantileQuantileData = \
    #             pd.DataFrame(quantileTheoryPractical, columns=["theoryQuantile",
    #                                                            "practicalQuantile"])
    #         quantileQuantileData = spark.createDataFrame(quantileQuantileData)
    #         quantileQuantileData.na.drop()
    #         print("Quantile plot : success")
    #     except:
    #         quantileQuantileData = spark.createDataFrame(sparkContext.emptyRDD(), schema)
    #         print("Quantile plot : failed")
    # 
    #     return quantileQuantileData

    # reverting labelIndexing to its original value
    @staticmethod
    def revertIndexToString(dataset, label, indexedLabel):
        from pyspark.sql.functions import regexp_replace
        distinctLabelDataset = dataset.select(label).distinct()
        listOfDistinctValues = list(distinctLabelDataset.select(label).toPandas()[label])
        # for now we are reverting the index column but actully
        # we have to replace the predicted column, so plan accordingly
        if (len(listOfDistinctValues) == len(indexedLabel)):
            for index, val in enumerate(indexedLabel):
                floatIndex = str(float(index))
                dataset = dataset.withColumn(label, regexp_replace(label, floatIndex, val))

    # removing the specialCharacters from the columns name for parquet writing.
    @staticmethod
    def removeSpecialCharacters(columnName):
        colName = re.sub('[^a-zA-Z0-9]', '_', columnName)
        return colName

    # method to create key value pair for statistics
    @staticmethod
    def statsDict(statList, statDict, idNameFeaturesOrdered):
        for index, value in enumerate(statList):
            statDict[index] = round(value, 4)

        return mainUtilities.summaryTable(featuresName=idNameFeaturesOrdered,
                                                featuresStat=statDict)

    # @staticmethod
    # def performETL(etlInfo):
    #     #fix this method when u work on prediction.
    #     datasetAdd = etlInfo.get(mc.DATASETADD)
    #     relationshipList = etlInfo.get(mc.RELATIONSHIP_LIST)
    #     relation = etlInfo.get(mc.RELATIONSHIP)
    #     trainDataRatio = etlInfo.get(mc.TRAINDATALIMIT)
    #     spark = etlInfo.get(mc.SPARK)
    #     #delete the prediction column if already existed---
    #     modelName = etlInfo.get(mc.MODELSHEETNAME)
    # 
    #     #check if the dataset is already passed by the method or not. if not then read the dataset from the obj
    #     dataset = etlInfo.get(mc.DATASET)
    #     if(dataset is None):
    #         dataset = spark.read.parquet(datasetAdd)
    #         originalDataset = None #setting this none in case of featureAnlaysis and training session
    #     else:
    #         originalDataset = dataset
    #         etlInfo.pop(mc.DATASET)
    # 
    #     if (modelName is not None):
    #         dataset = dataset.drop(modelName)
    # 
    #     # changing the relationship of the colm
    #     dataTransformationObj = PredictiveDataTransformation(dataset=dataset)
    #     dataset = \
    #         dataTransformationObj.colmTransformation(
    #             colmTransformationList=relationshipList) if relation == mc.NON_LINEAR else dataset
    #     # transformation
    #     dataTransformationObj = PredictiveDataTransformation(dataset=dataset)
    #     dataTransformationResult = dataTransformationObj.dataTranform(etlInfo)
    #     dataset = dataTransformationResult[mc.DATASET]
    #     categoricalFeatures = dataTransformationResult.get(mc.CATEGORICALFEATURES)
    #     numericalFeatures = dataTransformationResult.get(mc.NUMERICALFEATURES)
    #     maxCategories = dataTransformationResult.get(mc.MAXCATEGORIES)
    #     categoryColmStats = dataTransformationResult.get(mc.CATEGORYCOLMSTATS)
    #     indexedFeatures = dataTransformationResult.get(mc.INDEXEDFEATURES)
    #     idNameFeaturesOrdered = dataTransformationResult.get(mc.IDNAMEFEATURESORDERED)
    #     oneHotEncodedFeaturesList = dataTransformationResult.get(mc.ONEHOTENCODEDFEATURESLIST)
    #     label = dataTransformationResult.get(mc.LABEL)
    #     featuresColm = dataTransformationResult.get(mc.VECTORFEATURES)
    #     isLabelIndexed = dataTransformationResult.get(mc.ISLABELINDEXED)
    #     # featuresColm = "features"
    # 
    #     if trainDataRatio is not None:
    #         trainData, testData = dataset.randomSplit([trainDataRatio, (1 - trainDataRatio)],
    #                                                   seed=40)
    #         ETLOnDatasetStat = {mc.FEATURESCOLM: featuresColm,
    #                             mc.LABELCOLM: label,
    #                             mc.TRAINDATA: trainData,
    #                             mc.TESTDATA: testData,
    #                             mc.IDNAMEFEATURESORDERED: idNameFeaturesOrdered,
    #                             mc.DATASET: dataset,
    #                             mc.INDEXEDFEATURES: indexedFeatures,
    #                             mc.ONEHOTENCODEDFEATURESLIST: oneHotEncodedFeaturesList,
    #                             mc.MAXCATEGORIES: maxCategories,
    #                             mc.CATEGORYCOLMSTATS: categoryColmStats,
    #                             mc.CATEGORICALFEATURES: categoricalFeatures,
    #                             mc.NUMERICALFEATURES: numericalFeatures,
    #                             mc.ISLABELINDEXED: isLabelIndexed,
    #                             mc.ORIGINALDATASET:originalDataset}
    #     else:
    #         ETLOnDatasetStat = {mc.FEATURESCOLM: featuresColm,
    #                             mc.LABELCOLM: label,
    #                             mc.IDNAMEFEATURESORDERED: idNameFeaturesOrdered,
    #                             mc.DATASET: dataset,
    #                             mc.INDEXEDFEATURES: indexedFeatures,
    #                             mc.ONEHOTENCODEDFEATURESLIST: oneHotEncodedFeaturesList,
    #                             mc.MAXCATEGORIES: maxCategories,
    #                             mc.CATEGORYCOLMSTATS: categoryColmStats,
    #                             mc.CATEGORICALFEATURES: categoricalFeatures,
    #                             mc.NUMERICALFEATURES: numericalFeatures,
    #                             mc.ISLABELINDEXED: isLabelIndexed,
    #                             mc.ORIGINALDATASET:originalDataset
    #                             }
    # 
    #     return ETLOnDatasetStat

    @staticmethod
    def addInternalId(dataset):
        dataset = dataset.drop(mc.PA_INDEX)
        # look for alternative of monotonically increasing Id function of spark
        dataset = dataset.withColumn(mc.PA_INDEX, F.monotonically_increasing_id())
        return dataset

    @staticmethod
    def joinDataset(datasetOne, datasetTwo, joinOnColumn):
        dataset = datasetOne.join(datasetTwo, on=[joinOnColumn]).sort(joinOnColumn)
        return dataset



    '''alternative of one hot encoding for sentiment analysis.'''
    @staticmethod
    def countVectorizer(infoData):
        colName = infoData.get(mc.COLMTOENCODE)
        dataset = infoData.get(mc.DATASET)
        encodedColm = infoData.get(mc.ENCODEDCOLM)
        originalColmName = infoData.get(mc.ORIGINALCOLMNAME)
        oneHotEncoderPathMapping = infoData.get(mc.ONEHOTENCODERPATHMAPPING)
        storageLocation = infoData.get(mc.STORAGELOCATION)
        countVectorizer = CountVectorizer(inputCol=colName,
                                          outputCol=encodedColm).fit(dataset)
        '''oneHotEncoderPath = storageLocation + modelId.upper() + PredictiveConstants.ONEHOTENCODED.upper() + PredictiveConstants.PARQUETEXTENSION
        oneHotEncoder.write().overwrite().save(oneHotEncoderPath)
        oneHotEncoderPathMapping.update({
            PredictiveConstants.ONEHOTENCODED: oneHotEncoderPath
        })'''

        oneHotEncoderPath = storageLocation +  mc.ONEHOTENCODED_.upper() + originalColmName.upper() + mc.PARQUETEXTENSION
        countVectorizer.write().overwrite().save(oneHotEncoderPath)
        oneHotEncoderPathMapping.update({
            originalColmName: oneHotEncoderPath
        })

        dataset = countVectorizer.transform(dataset)
        infoData.update({
            mc.ONEHOTENCODERPATHMAPPING: oneHotEncoderPathMapping,
            mc.DATASET: dataset
        })
        return infoData

    @staticmethod
    def featureAssembler(infoData):
        '''requires list of colms to vectorized , dataset and output colm name for feature'''
        colList = infoData.get(mc.COLMTOVECTORIZED)
        if(isinstance(colList, str)):
            colList = [colList]
        dataset = infoData.get(mc.DATASET)
        featuresColm = infoData.get(mc.FEATURESCOLM)
        dataset = dataset.drop(featuresColm)

        featureassembler = VectorAssembler(
            inputCols=colList,
            outputCol=featuresColm, handleInvalid="skip")
        dataset = featureassembler.transform(dataset)
        infoData.update({
            mc.DATASET: dataset
        })
        return infoData

    @staticmethod
    def stringIndexer(infoData):
        colmToIndex = infoData.get(mc.COLMTOINDEX)
        dataset = infoData.get(mc.DATASET)
        indexedColm = infoData.get(mc.INDEXEDCOLM)
        storageLocation = infoData.get(mc.STORAGELOCATION)
        indexerName = colmToIndex + mc.INDEXER
        file = storageLocation + indexerName
        # check if the datatype of the col is integer or float or double. if yes then no need to do the indexing-- sahil.
        '''for now converting each datatypes to string and then indexing it.'''
        dataset = dataset.withColumn(colmToIndex, dataset[colmToIndex].cast(StringType()))
        stringIndexer = StringIndexer(inputCol=colmToIndex, outputCol=indexedColm,
                                     handleInvalid="keep").fit(dataset)
        dataset = stringIndexer.transform(dataset)
        stringIndexer.write().overwrite().save(file)  # will update this later
        indexerPathMapping = infoData.get(mc.INDEXERPATHMAPPING)
        indexerPathMapping.update({colmToIndex: file})
        infoData.update({
            mc.INDEXERPATHMAPPING: indexerPathMapping,
            mc.DATASET: dataset
        })

        return infoData

    @staticmethod
    def indexToString(infoData):
        stringIndexerPath = infoData.get(mc.INDEXERPATH)
        inverterColm = infoData.get(mc.COLMTOINVERT)
        dataset = infoData.get(mc.DATASET)
        stringIndexer = StringIndexerModel.load(stringIndexerPath)
        inverter = IndexToString(inputCol=inverterColm, outputCol=mc.DMXINVERTEDCOLM,
                                 labels=stringIndexer.labels)
        dataset = inverter.transform(dataset)

        #drop the indexed colm and rename the new unindexed colm with the actual one
        dataset = dataset.drop(inverterColm)
        dataset = dataset.withColumnRenamed(mc.DMXINVERTEDCOLM, inverterColm)
        return dataset

    @staticmethod
    def writeToJson(storageLocation, data):
        extension = ".json"
        mode = "w"  # write
        json.dump(data, open(storageLocation + extension, mode))

    @staticmethod
    def duplicateDataset(dataset):
        dataset = mainUtilities.addInternalId(dataset)
        duplicateDataset = dataset
        return dataset, duplicateDataset

    """
    #write to csv
        # finalDataset.drop(mc.DMXSTEMMEDWORDS, mc.DMXSTOPWORDS, mc.DMXTOKENIZED, mc.DMXTAGGEDCOLM,mc.DMXSTEMMEDWORDS, mc.DMXNGRAMS).coalesce(
        #     1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").csv(
        #     "/home/fidel/Documents/knimeStopWordsRohitSirDataWithStem.csv")
    """

    # replace the value of a column based on some condition
    """
    datasetDL = datasetDL.withColumn("original_sentiment_redefined", 
    when(datasetDL["original_sentiment"] == "POS", "positive")
    .when(datasetDL["original_sentiment"] == "NEG", "negative")).show()
    """