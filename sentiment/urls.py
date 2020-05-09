from django.urls import path, include
from sentiment.views import SentimentAnalysis

urlpatterns = [
    path('', SentimentAnalysis.sentimentHome),
    path(r'', include('viveknSentiment.urls'))
]