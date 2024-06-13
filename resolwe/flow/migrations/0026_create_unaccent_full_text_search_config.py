# Generated by Django 4.2.9 on 2024-02-13 07:01

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("flow", "0025_add_unaccent_extension"),
    ]

    operations = [
        migrations.RunSQL(
            """
            DO
            $$BEGIN
                CREATE TEXT SEARCH CONFIGURATION simple_unaccent( COPY = simple );
            EXCEPTION
                WHEN unique_violation THEN
                    NULL;  -- ignore error
            END;$$;
            """
        ),
        migrations.RunSQL(
            "ALTER TEXT SEARCH CONFIGURATION simple_unaccent "
            + "ALTER MAPPING FOR hword, hword_part, word "
            + "WITH unaccent, simple;"
        ),
    ]