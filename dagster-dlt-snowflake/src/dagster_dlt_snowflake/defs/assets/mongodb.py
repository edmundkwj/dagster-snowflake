from dagster import AssetExecutionContext, Definitions
from dagster_embedded_elt.dlt import DagsterDltResource, dlt_assets

import dlt
from ...sources.mongodb import mongodb


mflix = mongodb(
    database='sample_mflix'
).with_resources(
    "comments",
    "embedded_movies"
)


@dlt_assets(
    dlt_source=mflix,
    dlt_pipeline=dlt.pipeline(
        pipeline_name="local_mongo",
        destination='snowflake',
        dataset_name="mflix",
    ),
    name="mongodb",
    group_name="mongodb",
)
def dlt_asset_factory(context: AssetExecutionContext, dlt: DagsterDltResource):
    context.log.info("Starting DLT pipeline run...")

    # Check if we can connect to MongoDB first
    try:
        # Test the source
        for resource_name in mflix.resources:
            context.log.info(f"Available resource: {resource_name}")
    except Exception as e:
        context.log.error(f"Error accessing MongoDB source: {e}")
        raise

    context.log.info("Running DLT pipeline...")
    yield from dlt.run(context=context, write_disposition="merge")
    context.log.info("DLT pipeline completed.")


# DLT resource will be defined in the main definitions file