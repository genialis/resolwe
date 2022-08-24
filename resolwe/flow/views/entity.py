"""Entity viewset."""
from django.core.exceptions import ValidationError as DjangoValidationError
from django.db import transaction
from django.db.models import Prefetch

from rest_framework import exceptions
from rest_framework.decorators import action
from rest_framework.response import Response

from resolwe.flow.filters import EntityFilter
from resolwe.flow.models import AnnotationValue, Collection, DescriptorSchema, Entity
from resolwe.flow.models.annotations import AnnotationField, HandleMissingAnnotations
from resolwe.flow.serializers import EntitySerializer
from resolwe.flow.serializers.annotations import (
    AnnotationsSerializer,
    AnnotationValueSerializer,
)
from resolwe.observers.mixins import ObservableMixin
from resolwe.process.descriptor import ValidationError

from .collection import BaseCollectionViewSet
from .utils import get_collection_for_user


class EntityViewSet(ObservableMixin, BaseCollectionViewSet):
    """API view for entities."""

    serializer_class = EntitySerializer
    filter_class = EntityFilter
    qs_collection_ds = DescriptorSchema.objects.select_related("contributor")
    qs_collection = Collection.objects.select_related("contributor")
    qs_collection = qs_collection.prefetch_related(
        "data",
        "entity_set",
        Prefetch("descriptor_schema", queryset=qs_collection_ds),
    )
    qs_descriptor_schema = DescriptorSchema.objects.select_related("contributor")
    queryset = Entity.objects.select_related("contributor").prefetch_related(
        "data",
        Prefetch("collection", queryset=qs_collection),
        Prefetch("descriptor_schema", queryset=qs_descriptor_schema),
    )

    def _get_entities(self, user, ids):
        """Return entities queryset based on provided entity ids."""
        queryset = Entity.objects.filter(id__in=ids).filter_for_user(user)
        actual_ids = queryset.values_list("id", flat=True)
        missing_ids = list(set(ids) - set(actual_ids))
        if missing_ids:
            raise exceptions.ParseError(
                "Entities with the following ids not found: {}".format(
                    ", ".join(map(str, missing_ids))
                )
            )

        return queryset

    @action(detail=False, methods=["post"])
    def move_to_collection(self, request, *args, **kwargs):
        """Move samples from source to destination collection.

        The request accepts argument 'handle_missing_annotations'.The possible
        values for it are 'ADD' and 'REMOVE'. When 'REMOVE' is set without
        arguments the 'confirm_annotations_delete' argument must be set to
        True.

        :raises ValidationError: when handle_missing_annotations has incorrect
            value.
        :raises ValidationError: when handle_missing_annotations is set to
            'REMOVE' and 'confirm_annotations_delete' is set.
        """

        ids = self.get_ids(request.data)
        dst_collection_id = self.get_id(request.data, "destination_collection")
        dst_collection = get_collection_for_user(dst_collection_id, request.user)
        entity_qs = self._get_entities(request.user, ids)

        missing_annotations = {
            entity: AnnotationField.objects.filter(collection=entity.collection)
            .exclude(collection=dst_collection)
            .distinct()
            for entity in entity_qs
        }

        if missing_annotations:
            # How to handle annotation fields present in the current collection
            # but missing in new one. We have two options:
            # - add (default): add the missing annotation fields to the new
            #   collection and keep the values.
            # - delete: remove the missing annotation values from the samples.
            #   This option requires explicit confirmation by 'confirm_remove'
            #   argument or exception with explanation is raised.
            try:
                handle_missing_annotations = HandleMissingAnnotations(
                    request.data.get(
                        "handle_missing_annotations", HandleMissingAnnotations.ADD.name
                    )
                )
            except ValueError:
                possible_values = HandleMissingAnnotations._member_names_
                raise ValidationError(
                    "Attribute 'handle_missing_annotations' must be one of "
                    f"{possible_values}."
                )

            if handle_missing_annotations == HandleMissingAnnotations.REMOVE:
                if request.data.get("confirm_annotations_remove", False) is not True:
                    raise ValidationError(
                        "All annotations not present in the target collection will be "
                        "removed. Set 'confirm_action' argument to 'True' to confirm."
                    )

        entity_qs.move_to_collection(dst_collection, handle_missing_annotations)
        return Response()

    # NOTE: This can be deleted when DRF will support select_for_update
    #       on updates and ResolweUpdateModelMixin will use it.
    #       https://github.com/encode/django-rest-framework/issues/4675
    def update(self, request, *args, **kwargs):
        """Update an entity.

        Original queryset produces a temporary database table whose rows
        cannot be selected for an update. As a workaround, we patch
        get_queryset function to return only Entity objects without
        additional data that is not needed for the update.
        """
        orig_get_queryset = self.get_queryset

        def patched_get_queryset():
            """Patched get_queryset method."""
            entity_ids = orig_get_queryset().values_list("id", flat=True)
            return Entity.objects.filter(id__in=entity_ids)

        self.get_queryset = patched_get_queryset

        resp = super().update(request, *args, **kwargs)

        self.get_queryset = orig_get_queryset
        return resp

    @action(detail=False, methods=["post"])
    def duplicate(self, request, *args, **kwargs):
        """Duplicate (make copy of) ``Entity`` models."""
        if not request.user.is_authenticated:
            raise exceptions.NotFound

        inherit_collection = request.data.get("inherit_collection", False)
        ids = self.get_ids(request.data)
        queryset = Entity.objects.filter(id__in=ids).filter_for_user(request.user)
        actual_ids = queryset.values_list("id", flat=True)
        missing_ids = list(set(ids) - set(actual_ids))
        if missing_ids:
            raise exceptions.ParseError(
                "Entities with the following ids not found: {}".format(
                    ", ".join(map(str, missing_ids))
                )
            )

        duplicated = queryset.duplicate(
            contributor=request.user, inherit_collection=inherit_collection
        )

        serializer = self.get_serializer(duplicated, many=True)
        return Response(serializer.data)

    @action(detail=True, methods=["post"])
    def set_annotations(self, request, pk=None):
        """Add the given list of AnnotaitonFields to the given collection."""
        # No need to check for permissions, since post requires edit by default.
        entity = self.get_object()
        # Read and validate the request data.
        serializer = AnnotationsSerializer(data=request.data, many=True)
        serializer.is_valid(raise_exception=True)
        annotations = [
            (entry["field"], entry["value"]) for entry in serializer.validated_data
        ]
        annotation_fields = [e[0] for e in annotations]
        # The following dict is a mapping from annotation field id to the annotation
        # value id.
        existing_annotations = dict(
            entity.annotations.filter(field__in=annotation_fields).values_list(
                "field_id", "id"
            )
        )

        validation_errors = []
        to_create = []
        to_update = []
        for field, value in annotations:
            annotation_id = existing_annotations.get(field.id)
            append_to = to_create if annotation_id is None else to_update
            annotation = AnnotationValue(
                entity_id=entity.id, field_id=field.id, value=value, id=annotation_id
            )
            try:
                annotation.validate()
            except DjangoValidationError as e:
                validation_errors += e
            append_to.append(annotation)

        if validation_errors:
            raise DjangoValidationError(validation_errors)

        with transaction.atomic():
            AnnotationValue.objects.bulk_create(to_create)
            AnnotationValue.objects.bulk_update(to_update, ["_value"])
        return Response()

    @action(detail=True, methods=["get"])
    def get_annotations(self):
        """Get the annotations on this sample."""
        entity = self.get_object()
        serializer = AnnotationValueSerializer(entity.annotations.all(), many=True)
        return Response(serializer.data)
