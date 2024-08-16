from dagster import define_asset_job, AssetSelection


movies_job = define_asset_job(
    "movies_job",
    selection=AssetSelection.all() - AssetSelection.groups("mongodb"),
)
