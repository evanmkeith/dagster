#!/usr/bin/env python3
"""
Test script to verify PR #32580 fix for Brian Lewis's enable_code_references error
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

def test_fix_with_code_references_enabled():
    """This should now work after applying PR #32580 fix"""
    print("=== Testing with enable_code_references=True (SHOULD NOW WORK) ===")
    
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

        # This line should now work after the fix
        source_keys = get_asset_keys_by_output_name_for_source([my_dbt_assets_with_code_refs], "raw_data")
        print(f"✅ SUCCESS: Retrieved source keys: {source_keys}")
        print("✅ Brian Lewis's error has been FIXED!")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {type(e).__name__}: {e}")
        print("❌ The fix may not be working correctly")
        return False

def test_with_code_references_disabled():
    """This should continue to work as before"""
    print("\n=== Testing with enable_code_references=False (SHOULD STILL WORK) ===")
    
    try:
        # Configure translator with enable_code_references=False
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

        # This should continue to work
        source_keys = get_asset_keys_by_output_name_for_source([my_dbt_assets_without_code_refs], "raw_data")
        print(f"✅ SUCCESS: Retrieved source keys: {source_keys}")
        return True
        
    except Exception as e:
        print(f"❌ UNEXPECTED ERROR: {type(e).__name__}: {e}")
        return False

def test_code_references_functionality():
    """Test that code references are actually being attached to assets"""
    print("\n=== Testing Code References Functionality ===")
    
    try:
        dagster_dbt_translator = DagsterDbtTranslator(
            settings=DagsterDbtTranslatorSettings(enable_code_references=True)
        )

        @dbt_assets(
            manifest=dbt_project.manifest_path,
            dagster_dbt_translator=dagster_dbt_translator,
            project=dbt_project,
        )
        def my_dbt_assets_with_refs(context: AssetExecutionContext, dbt: DbtCliResource):
            yield from dbt.cli(["build"], context=context).stream()

        # Check if code references are attached to the assets
        for asset_key, asset_metadata in my_dbt_assets_with_refs.metadata_by_key.items():
            if "dagster/code_references" in asset_metadata:
                print(f"✅ Code references found for asset: {asset_key}")
                references = asset_metadata["dagster/code_references"].code_references
                for ref in references:
                    if hasattr(ref, 'file_path'):
                        print(f"   📄 File: {ref.file_path}")
                    if hasattr(ref, 'url'):
                        print(f"   🔗 URL: {ref.url}")
                return True
            else:
                print(f"⚠️  No code references found for asset: {asset_key}")
        
        return False
        
    except Exception as e:
        print(f"❌ ERROR testing code references: {type(e).__name__}: {e}")
        return False

if __name__ == "__main__":
    print("=== PR #32580 Fix Verification ===")
    print(f"Using dagster-dbt version: 0.27.15 (with applied fix)")
    print(f"dbt project path: {dbt_project_path}")
    
    # Test both scenarios
    test1_passed = test_fix_with_code_references_enabled()
    test2_passed = test_with_code_references_disabled()
    test3_passed = test_code_references_functionality()
    
    print("\n" + "="*50)
    print("FINAL RESULTS:")
    print(f"✅ enable_code_references=True works: {'YES' if test1_passed else 'NO'}")
    print(f"✅ enable_code_references=False works: {'YES' if test2_passed else 'NO'}")
    print(f"✅ Code references functionality: {'YES' if test3_passed else 'NO'}")
    
    if test1_passed and test2_passed:
        print("\n🎉 SUCCESS: PR #32580 fix has resolved Brian Lewis's issue!")
        print("👍 The get_asset_keys_by_output_name_for_source() function now works with enable_code_references=True")
    else:
        print("\n❌ Some tests failed - the fix may need additional work")