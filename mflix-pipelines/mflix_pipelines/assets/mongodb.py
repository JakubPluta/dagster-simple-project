from dagster import AssetExecutionContext
from dagster_embedded_elt.dlt import dlt_asset, DagsterDltResource
from .. import mongodb as dlt_mongodb
import dlt 



DATABASE_NAME = "sample_mflix"
COLLECTIONS = ["comments", "embedded_movies", "movies",]
WRIITE_DISPOSITION = "merge"

mflix_mongodb_connection = dlt_mongodb.mongodb(
    database=DATABASE_NAME
).with_resources(
    *COLLECTIONS
)


@dlt_asset(
    dlt_source=mflix_mongodb_connection,
    dlt_pipeline=dlt.pipeline(
        pipeline_name="mongodb_pipeline",
        destination="snowflake",
        dataset_name="mflix",
        progress="log",
    ),
    name="mflix_mongodb",
    group_name="mflix_mongodb",
)
def dlt_asset_factory(
    context: AssetExecutionContext,
    dlt: DagsterDltResource
    ):
    yield from dlt.run(
        context=context, 
        write_disposition=WRIITE_DISPOSITION
        )