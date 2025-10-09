from dagster import Config
from dagster import DynamicOut
from dagster import DynamicOutput
from dagster import RunConfig
from dagster import define_asset_job
from dagster import graph_asset
from dagster import op


class FanOutConfig(Config):
    value: int


class ProcessAssetConfig(Config):
    value: int


class FanInConfig(Config):
    value: int


@op(out=DynamicOut(), name="fan_out_asset")
def fan_out_asset(config: FanOutConfig):
    yield DynamicOutput(value=1, mapping_key="first_asset")


@op(name="process_asset")
def process_asset(value: int, config: ProcessAssetConfig):
    return value


@op(name="fan_in_asset")
def fan_in(processed_asset: list[int], config: FanInConfig):
    return sum(processed_asset)


@graph_asset(name="my_graph_asset")
def my_graph_asset():
    second_asset = fan_out_asset()
    processed_output = second_asset.map(process_asset)
    return fan_in(processed_output.collect())


broken_assets_job = define_asset_job(
    name="my_graph_asset_job",
    selection=[my_graph_asset],
    config=RunConfig(
        ops={
            "my_graph_asset": {
                "ops": {
                    "fan_out_asset": FanOutConfig(value=1),
                    "process_asset": ProcessAssetConfig(value=1),
                    "fan_in_asset": FanInConfig(value=1),
                }
            },
        }
    ),
)