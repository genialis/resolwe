from django.conf.urls import include, url


urlpatterns = [
    url(r'^', include('resolwe.api_urls')),
    url(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework'))
]
