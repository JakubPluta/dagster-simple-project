from dagster import Definitions, EnvVar, load_assets_from_modules

from .assets import mongodb
from .assets import movies
from dagster_embedded_elt.dlt import DagsterDltResource
from dagster_snowflake import SnowflakeResource


mongodb_assets = load_assets_from_modules([mongodb])
movies_assets = load_assets_from_modules([movies])

dagster_dlt_resource = DagsterDltResource()
snowflake_resource = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    warehouse="dagster_wh",
    database="dagster_db",
    schema="mflix",
    role="dagster_role",
)


defs = Definitions(
    assets=[*mongodb_assets, *movies_assets],
    resources={"dlt": dagster_dlt_resource, "snowflake": snowflake_resource},
)
