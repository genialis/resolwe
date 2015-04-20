from django.conf.urls import include, url


urlpatterns = [
    url(r'^', include('resolwe.api_urls')),
]
