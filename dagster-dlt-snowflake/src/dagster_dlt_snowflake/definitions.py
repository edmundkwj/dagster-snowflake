import os
from pathlib import Path

# Load environment variables from .env file FIRST
try:
    from dotenv import load_dotenv
    env_file = Path(__file__).parent.parent.parent.parent / ".env"
    if env_file.exists():
        load_dotenv(env_file)
except ImportError:
    pass

from .defs.assets import mongodb, movies, adhoc
from .defs.jobs import movies_job, adhoc_job
from .defs.schedules import movies_schedule
from .defs.sensors import adhoc_sensor

from dagster import Definitions, load_assets_from_modules, EnvVar
from dagster_embedded_elt.dlt import DagsterDltResource
from dagster_snowflake import SnowflakeResource

mongodb_assets = load_assets_from_modules([mongodb])
movies_assets = load_assets_from_modules([movies], group_name="movies")
adhoc_assets = load_assets_from_modules([adhoc], group_name="ad_hoc")

snowflake = SnowflakeResource(
    account=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__HOST"),
    user=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__USERNAME"),
    password=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__PASSWORD"),
    warehouse=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__WAREHOUSE"),
    database=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__DATABASE"),
    role=EnvVar("DESTINATION__SNOWFLAKE__CREDENTIALS__ROLE"),
)

defs = Definitions(
    assets=[*mongodb_assets, *movies_assets, *adhoc_assets], # The * means import everything
    resources={
        "dlt": DagsterDltResource(),
        "snowflake": snowflake
    },
    jobs=[movies_job, adhoc_job],
    schedules=[movies_schedule],
    sensors=[adhoc_sensor]
)
