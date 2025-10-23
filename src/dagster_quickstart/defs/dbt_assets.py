from pathlib import Path
from dagster import AssetExecutionContext, Definitions
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
    get_asset_keys_by_output_name_for_source,
)

# Set up dbt project with robust path handling
dbt_project_path = Path(__file__).parent.parent.parent.parent / "dbt_project"

# Only create DbtProject if the directory exists
if dbt_project_path.exists():
    dbt_project = DbtProject(project_dir=dbt_project_path)
    
    # Configure translator with enable_code_references=True
    dagster_dbt_translator = DagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_code_references=True)
    )
    
    # Check if manifest exists, otherwise let dbt auto-discover
    manifest_path = dbt_project.manifest_path if dbt_project.manifest_path.exists() else None
    
    @dbt_assets(
        manifest=manifest_path,
        dagster_dbt_translator=dagster_dbt_translator,
        project=dbt_project,
    )
    def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
        """dbt assets for transforming raw data"""
        yield from dbt.cli(["build"], context=context).stream()

else:
    # Fallback: Create a dummy asset if dbt project doesn't exist
    from dagster import asset
    
    @asset(group_name="dbt_fallback")
    def my_dbt_assets():
        """Placeholder asset when dbt project is not available"""
        return {"status": "dbt_project_not_found", "message": "dbt project directory not found"}


# This is the line that should trigger Brian Lewis's error in buggy versions
# get_asset_keys_by_output_name_for_source calls get_asset_spec with None as project parameter
# when enable_code_references=True, this should fail in versions before the fix
def test_source_asset_keys():
    """This function will trigger the error that Brian Lewis reported"""
    try:
        # This should fail in versions before 1.11.10 when enable_code_references=True
        source_keys = get_asset_keys_by_output_name_for_source([my_dbt_assets], "raw_data")
        print(f"SUCCESS: Retrieved source keys: {source_keys}")
        return source_keys
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        raise


# dbt resource configuration - only if dbt project exists
if dbt_project_path.exists():
    dbt_resource = DbtCliResource(project_dir=dbt_project_path)
else:
    dbt_resource = None

# Export the dbt assets and resource for use in main definitions
dbt_assets_list = [my_dbt_assets]
dbt_resources = {"dbt": dbt_resource} if dbt_resource else {}

# Standalone definitions removed to avoid conflicts with serverless_definitions.py
# The dbt assets and resources are exported for inclusion in the main definitions