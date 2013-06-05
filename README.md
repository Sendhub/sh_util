requirements:

    - settings.py in the base path of your app
        With the following directives:
            SH_UTIL_DB_DRIVER = 'django' | 'sqlalchemy'

            SH_UTIL_USE_PERSISTENT_DBLINK = os.getenv('SH_UTIL_USE_PERSISTENT_DBLINK', '') == '1'

            AWS_STORAGE_BUCKET_NAME = ''

    - Python >= 2.7
    - DB Driver: Django or SQLAlchemy
    - SQL Parse lib from: git+git://github.com/Sendhub/sqlparse.git@betterAliasDetection


pip requirements.txt:

    phonenumbers
    boto
    psycopg2==2.4.6
    pylibmc==1.2.3
    git+git://github.com/Sendhub/sqlparse.git@betterAliasDetection
