from django import template

register = template.Library()


@register.filter
def increase(value):
    return value + 1
