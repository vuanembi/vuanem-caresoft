import os
from datetime import datetime

import jinja2
from google.cloud import bigquery

from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# API Headers
HEADERS = {
    "Authorization": f"Bearer {os.getenv('ACCESS_TOKEN')}",
    "Content-Type": "application/json",
}
BASE_URL = "https://api.caresoft.vn/VUANEM/api/v1"

# API Calls Configs
COUNT = 500
DETAILS_LIMIT = 2500

# BigQuery Configs
BQ_CLIENT = bigquery.Client()
DATASET = "Caresoft"

# Datetime Formatting
NOW = datetime.utcnow()
DATE_FORMAT = "%Y-%m-%d"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Jinja2 Configs
TEMPLATE_LOADER = jinja2.FileSystemLoader(searchpath="./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)

ENGINE = create_engine(
    URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("PG_UID"),
        password=os.getenv("PG_PWD"),
        host=os.getenv("PG_HOST"),
        database=os.getenv("PG_DB"),
    ),
    executemany_mode="values",
    executemany_values_page_size=1000,
)
