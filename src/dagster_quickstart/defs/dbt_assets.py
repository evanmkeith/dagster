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

# Set up dbt project
dbt_project_path = Path(__file__).parent.parent.parent.parent / "dbt_project"
dbt_project = DbtProject(project_dir=dbt_project_path)

# Configure translator with enable_code_references=True
dagster_dbt_translator = DagsterDbtTranslator(
    settings=DagsterDbtTranslatorSettings(enable_code_references=True)
)

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=dagster_dbt_translator,
    project=dbt_project,
)
def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


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


defs = Definitions(
    assets=[my_dbt_assets],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project_path),
    },
)