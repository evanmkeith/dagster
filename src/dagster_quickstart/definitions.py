from pathlib import Path
import dagster as dg

# Load assets and asset checks from the defs folder
from . import defs

# Create the main Definitions object that Dagster Cloud will discover
defs = dg.Definitions(
    assets=dg.load_assets_from_modules([defs]),
    asset_checks=dg.load_asset_checks_from_modules([defs]),
)