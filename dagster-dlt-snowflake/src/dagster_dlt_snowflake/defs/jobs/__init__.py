
from dagster import AssetSelection, define_asset_job
from ..partitions import monthly_partition

movies_job = define_asset_job(
    name="movies_job",
    selection=AssetSelection.all() - AssetSelection.groups("mongodb")
)

adhoc_job = define_asset_job(
    name="adhoc_job",
    selection=AssetSelection.assets(["movie_embeddings"]) | AssetSelection.groups("mongodb")
)