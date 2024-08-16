from dagster import Definitions, load_assets_from_modules

from .assets import mongodb
from dagster_embedded_elt.dlt import DagsterDltResource

mongodb_assets = load_assets_from_modules([mongodb])

defs = Definitions(
    assets=[*mongodb_assets],
    resources={"dlt": DagsterDltResource()},
)
