from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt, csrf_protect


class SentimentAnalysis:
    def __init__(self):
        #right now keeping it empty.
        pass

    @staticmethod
    @csrf_protect
    @csrf_exempt
    def sentimentResult(request):
        sentimentText = request.POST.get("sentimentText")
        print(sentimentText)
        responseData = {}
        responseData['result'] = "positive"
        responseData['message'] = "successfully get the responseck from the baend"
        return JsonResponse(responseData)

    @staticmethod
    def sentimentHome(request):
        return render(request, "SAHomePage.html")