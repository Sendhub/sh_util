SH_UTIL
===============

This submodule is for sendhub internal services. This acts as a common utility folder.
Applications like Inforeach, Billing, Admin is utilizing this submodule.

Table Of Contents
---------------

- [SH\_UTIL](#sh_util)
  - [Table Of Contents](#table-of-contents)
  - [Installation](#installation)
  - [Usage](#usage)
    - [Prequisites](#prequisites)
  - [Features](#features)
  - [Contributing](#contributing)

Installation
---------------

Use the below method to install.

Content of `requirement.txt`

```text
boto3==1.40.52
phonenumbers==9.0.16
psycopg2==2.9.11
pylibmc==1.6.3
git+https://github.com/Sendhub/sqlparse.git@dipayanray-sync-upstream-2025-10
```

Content of `requirement-dev.txt` (Optional)

```text
autoflake==2.3.1
black==25.9.0
isort==7.0.0
```

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

Usage
---------------

### Prequisites

- The root of the project must contain a file called `settings.py`
- In case the root does not contain settings.py then the file should be injected in root at the time of environment building
- Below values should be present in `settings.py` file to use SH_UTIL properly.
  - SH_UTIL_DB_DRIVER = 'django' | 'sqlalchemy'
  - SH_UTIL_USE_PERSISTENT_DBLINK = os.getenv('SH_UTIL_USE_PERSISTENT_DBLINK', '') == '1'
  - AWS_STORAGE_BUCKET_NAME = ''
  - STATIC_TABLES = ('sequence', 'of', 'static', 'db', 'tables', 'which', 'should', 'not', 'be', 'replicated')
  - SHARDING_IGNORE_TABLES = ('sequence', 'of', 'additional', 'tables', 'to', 'ignore')

Usage example

```python
import dj_database_url
from sh_util.text import ec2HostnameToIp as _ec2HostnameToIp
DATABASES["default"] = dj_database_url.config(env="", default=_ec2HostnameToIp(os.getenv("DATABASE_URL", "postgres://localhost/sendhub")))
```

Features
---------------

- Crypto
- Database
- HTTP
- Email
- Memcache
- AWS S3
- Siftscience Fraud Detection
- Kazoo Voice
- Miscleneous

Contributing
---------------

Be a part of the Dev Team.
