from django.urls import path
from viveknSentiment.views import ViveknViews

urlpatterns = [
    path(r'sentimentResult/viveknSentiment', ViveknViews.sentimentResult)
]