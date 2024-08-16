from dagster import Definitions, EnvVar, load_assets_from_modules

from .assets import mongodb
from .assets import movies
from .resources import dagster_dlt_resource, snowflake_resource
from .jobs import movies_job
from .schedules import movies_schedule
from dagster_embedded_elt.dlt import DagsterDltResource
from dagster_snowflake import SnowflakeResource


mongodb_assets = load_assets_from_modules([mongodb])
movies_assets = load_assets_from_modules([movies])


defs = Definitions(
    assets=[*mongodb_assets, *movies_assets],
    resources={"dlt": dagster_dlt_resource, "snowflake": snowflake_resource},
    jobs=[movies_job],
    schedules=[movies_schedule],
)
