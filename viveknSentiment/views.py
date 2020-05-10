from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from viveknSentiment.viveknConstants import viveknConstants as vc
from SentimentAnalysis.main.mainConstants import mainConstants as mc
from viveknSentiment.viveknMain.viveknStart import viveknStart
from sentiment.sentimentAnalysis.sentimentConstants import sentimentConstants as sc
from SentimentAnalysis.main.CreateSparkSession import CreateSparkSession
# from SentimentAnalysis.main.SigletoneClass import createSparkSessionTest


class ViveknViews:
    def __init__(self):
        self.spark = CreateSparkSession().getInstance().getSparkSession()
        # self.spark = createSparkSessionTest().getSparkSession()

    @staticmethod
    @csrf_exempt
    def sentimentResult(request):
        responseData = {}
        sentimentText = request.POST.get("sentimentText")
        viveknViews = ViveknViews()
        infoData = viveknViews.createData(sentimentText)
        try:
            responseData = viveknStart().viveknSentiment(infoData)
            responseData["run_status"] = "ERROR"

        except Exception as e:
            print(str(e))
            responseData["run_status"] = "ERROR"

        finally:
            return JsonResponse(responseData)

    """ this method needs to be written in another class. """
    def createData(self, sentimentText):
        infoData = {
            vc.SENTIMENTTEXT:sentimentText,
            vc.EXPLAINDOCPATH: "/home/fidel/cache_pretrained/explainDocumentDL",
            vc.VIVEKNPRETRAINEDPATH: "/home/fidel/cache_pretrained/viveknPretrainedModel",
            mc.SPARK: self.spark,
            sc.SENTIMENTCOLNAME: "text",
            mc.PREDICTIONCOL: "vivekn_prediction"
        }
        return infoData
