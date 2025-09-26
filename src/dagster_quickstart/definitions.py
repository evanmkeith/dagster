from pathlib import Path
import dagster as dg

# Load assets from the defs folder using load_assets_from_modules
from . import defs

# Create the main Definitions object that Dagster Cloud will discover
defs = dg.Definitions(
    assets=dg.load_assets_from_modules([defs]),
)