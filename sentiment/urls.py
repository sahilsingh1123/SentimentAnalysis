from django.urls import path
from sentiment.views import SentimentAnalysis

urlpatterns = [
    path('', SentimentAnalysis.sentimentHome),
    path(r'sentimentResult/',SentimentAnalysis.sentimentResult)
]