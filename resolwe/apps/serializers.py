from rest_framework import serializers

from .models import Package, App


class PackageSerializer(serializers.ModelSerializer):
    class Meta:
        model = Package
        fields = ('slug', 'title', 'created', 'modified', 'created_by', 'version', 'modules', 'index')


class AppSerializer(serializers.ModelSerializer):
    class Meta:
        model = App
        fields = ('slug', 'title', 'created', 'modified', 'created_by', 'package', 'modules', 'projects',
                  'default_project', 'description')
