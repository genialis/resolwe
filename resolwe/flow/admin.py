"""Admin pages."""

from django import forms
from django.contrib import admin
from django.contrib.admin import widgets
from django.contrib.auth.models import Group

from resolwe.flow.models import AnnotationField, AnnotationGroup, AnnotationPreset
from resolwe.permissions.models import Permission

admin.site.site_header = "Genialis Administration"
admin.site.site_title = "Genialis"


class AnnotationPresetForm(forms.ModelForm):
    """Form for AnnotationPreset admin page."""

    # Use FilteredSelectMultiple widget for filtering the annotation fields.
    fields = forms.ModelMultipleChoiceField(
        widget=widgets.FilteredSelectMultiple(
            verbose_name="Annotation fields", is_stacked=False
        ),
        queryset=AnnotationField.objects.all(),
    )

    # Groups with permission to view the preset.
    groups = forms.MultipleChoiceField(
        widget=widgets.FilteredSelectMultiple(verbose_name="Groups", is_stacked=False),
        # Required must be set to false to allow unselecting all entries.
        required=False,
    )

    class Meta:
        """Define model and fields for the form."""

        model = AnnotationPreset
        fields = ["name", "fields", "groups"]

    def __init__(self, *args, **kwargs):
        """Set the groups choices and initial value."""
        super().__init__(*args, **kwargs)
        group_choices = Group.objects.all().values_list("pk", "name").order_by("name")
        selected_group_ids = []
        if "instance" in kwargs:
            preset = kwargs["instance"]
            selected_group_ids = [
                group.pk for group in preset.groups_with_permission(Permission.VIEW)
            ]
        self.fields["groups"].choices = group_choices
        self.fields["groups"].initial = selected_group_ids

    def save(self, commit):
        """Save changes made to the preset."""
        preset = super().save(commit)
        # Remove all permissions on the preset.
        preset.permission_group.permissions.all().delete()
        # Add permission to selected groups.
        for group in Group.objects.filter(pk__in=self.cleaned_data["groups"]):
            preset.set_permission(Permission.VIEW, group)
        return preset


class AnnotationPresetAdmin(admin.ModelAdmin):
    """Admin page for AnnotationPreset."""

    form = AnnotationPresetForm


class AnnotationFieldAdmin(admin.ModelAdmin):
    """Admin page for AnnotationField."""

    search_fields = ["name", "group__name"]
    list_filter = ["group"]


admin.site.register(AnnotationField, AnnotationFieldAdmin)
admin.site.register(AnnotationGroup)
admin.site.register(AnnotationPreset, AnnotationPresetAdmin)
