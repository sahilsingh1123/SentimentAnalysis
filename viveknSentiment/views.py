from django.views.decorators.csrf import csrf_exempt, csrf_protect
from django.http import JsonResponse
from viveknSentiment.viveknConstants import viveknConstants as vc
from mainConstants import mainConstants as mc
from viveknSentiment.viveknMain.viveknStart import viveknStart
from pyspark.sql import SparkSession
# Create your views here.
# sparknlp.jar path need to set dynamic before committing it...
spark = \
    SparkSession.builder.appName('DMXPredictiveAnalytics')\
        .config("spark.jars", "/home/fidel/cache_pretrained/sparknlpFATjar.jar")\
        .master('local[*]').getOrCreate() #sahil- fix the sparknlpJar location
spark.sparkContext.setLogLevel('ERROR')

class viveknViews:
    def __init__(self):
        pass

    @staticmethod
    @csrf_exempt
    def sentimentResult(request):
        responseData = {}
        infoData = viveknViews.createData(request)
        try:
            responseData = viveknStart().viveknSentiment(infoData)
            responseData["run_status"] = "ERROR"

        except Exception as e:
            print(str(e))
            responseData["run_status"] = "ERROR"

        finally:
            #stop the sparkSession
            spark.stop()
            return JsonResponse(responseData)

    @staticmethod
    @csrf_exempt
    def createData(request):
        #add the path for other things like explainDocumentDL, and viveknPretrainedModel.
        sentimentText = request.POST.get("sentimentText")
        infoData = {vc.SENTIMENTTEXT:sentimentText,
                    mc.SPARK: spark}
        return infoData
