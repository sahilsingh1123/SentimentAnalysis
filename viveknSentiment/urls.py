from django.urls import path
from viveknSentiment.views import viveknViews

urlpatterns = [
    path(r'sentimentResult/viveknSentiment', viveknViews.sentimentResult)
]