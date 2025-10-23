#!/usr/bin/env python3
"""
Test script to reproduce Brian Lewis's enable_code_references error
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtProject,
    dbt_assets,
    get_asset_keys_by_output_name_for_source,
)
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource

# Set up dbt project
dbt_project_path = Path(__file__).parent / "dbt_project"
dbt_project = DbtProject(project_dir=dbt_project_path)

def test_with_code_references_enabled():
    """This should fail with the error Brian Lewis reported"""
    print("=== Testing with enable_code_references=True (SHOULD FAIL) ===")
    
    try:
        # Configure translator with enable_code_references=True
        dagster_dbt_translator = DagsterDbtTranslator(
            settings=DagsterDbtTranslatorSettings(enable_code_references=True)
        )

        @dbt_assets(
            manifest=dbt_project.manifest_path,
            dagster_dbt_translator=dagster_dbt_translator,
            project=dbt_project,
        )
        def my_dbt_assets_with_code_refs(context: AssetExecutionContext, dbt: DbtCliResource):
            yield from dbt.cli(["build"], context=context).stream()

        # This line should trigger the error
        source_keys = get_asset_keys_by_output_name_for_source([my_dbt_assets_with_code_refs], "raw_data")
        print(f"UNEXPECTED SUCCESS: Retrieved source keys: {source_keys}")
        
    except Exception as e:
        print(f"EXPECTED ERROR: {type(e).__name__}: {e}")
        print("This is the exact error Brian Lewis reported!")

def test_with_code_references_disabled():
    """This should work fine as a workaround"""
    print("\n=== Testing with enable_code_references=False (SHOULD WORK) ===")
    
    try:
        # Configure translator with enable_code_references=False (workaround)
        dagster_dbt_translator = DagsterDbtTranslator(
            settings=DagsterDbtTranslatorSettings(enable_code_references=False)
        )

        @dbt_assets(
            manifest=dbt_project.manifest_path,
            dagster_dbt_translator=dagster_dbt_translator,
            project=dbt_project,
        )
        def my_dbt_assets_without_code_refs(context: AssetExecutionContext, dbt: DbtCliResource):
            yield from dbt.cli(["build"], context=context).stream()

        # This should work fine
        source_keys = get_asset_keys_by_output_name_for_source([my_dbt_assets_without_code_refs], "raw_data")
        print(f"SUCCESS: Retrieved source keys: {source_keys}")
        print("Workaround successful - disabling code references allows it to work!")
        
    except Exception as e:
        print(f"UNEXPECTED ERROR: {type(e).__name__}: {e}")

if __name__ == "__main__":
    print("Reproducing Brian Lewis's enable_code_references error...")
    print(f"Using dagster-dbt version: 0.27.15")
    print(f"dbt project path: {dbt_project_path}")
    
    test_with_code_references_enabled()
    test_with_code_references_disabled()
    
    print("\n=== SUMMARY ===")
    print("- The error occurs when enable_code_references=True")  
    print("- The error happens in get_asset_keys_by_output_name_for_source()")
    print("- The function calls get_asset_spec() with None as project parameter")
    print("- When enable_code_references=True, a non-None project is required")
    print("- WORKAROUND: Set enable_code_references=False until dagster-dbt is updated")