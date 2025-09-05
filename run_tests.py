#!/usr/bin/env python3
"""
Test Suite Runner for Real Estate Direct Mail Processing System

This script runs the comprehensive test suite and provides detailed reporting
on test coverage, performance, and system reliability.
"""

import subprocess
import sys
import time
from pathlib import Path
import argparse

def run_command(cmd, description):
    """Run a command and return success status"""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {cmd}")
    print('='*60)
    
    start_time = time.time()
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        elapsed = time.time() - start_time
        
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        print(f"\nCompleted in {elapsed:.2f} seconds")
        print(f"Exit code: {result.returncode}")
        
        return result.returncode == 0
        
    except Exception as e:
        print(f"ERROR: Failed to run command: {e}")
        return False

def main():
    """Main test runner"""
    parser = argparse.ArgumentParser(description="Run comprehensive test suite")
    parser.add_argument("--fast", action="store_true", help="Run only fast tests")
    parser.add_argument("--component", choices=["property", "config", "processing", "integration", "performance"], 
                       help="Run tests for specific component only")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    # Ensure we're in the correct directory
    script_dir = Path(__file__).parent
    if script_dir != Path.cwd():
        print(f"Changing to script directory: {script_dir}")
        import os
        os.chdir(script_dir)
    
    print("REAL ESTATE DIRECT MAIL PROCESSING - TEST SUITE")
    print("=" * 80)
    print(f"Test execution started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Working directory: {Path.cwd()}")
    
    verbose_flag = "-v" if args.verbose else ""
    fast_flag = "-m 'not slow'" if args.fast else ""
    
    test_suites = []
    
    if not args.component or args.component == "property":
        test_suites.append({
            'name': 'Property Processor Tests',
            'file': 'test_property_processor.py',
            'description': 'Core property classification, priority scoring, and business logic'
        })
    
    if not args.component or args.component == "config":
        test_suites.append({
            'name': 'Multi-Region Configuration Tests', 
            'file': 'test_multi_region_config.py',
            'description': 'Region configuration loading, validation, and FIPS checking'
        })
    
    if not args.component or args.component == "processing":
        test_suites.append({
            'name': 'Monthly Processing Tests',
            'file': 'test_monthly_processing_v2.py', 
            'description': 'Multi-region orchestration and niche list integration'
        })
    
    if not args.component or args.component == "integration":
        test_suites.append({
            'name': 'Integration Tests',
            'file': 'test_integration.py',
            'description': 'End-to-end workflows and system reliability'
        })
    
    if not args.component or args.component == "performance":
        test_suites.append({
            'name': 'Performance Tests',
            'file': 'test_performance.py',
            'description': 'Performance optimizations and memory usage'
        })
    
    results = []
    total_start_time = time.time()
    
    for suite in test_suites:
        # Check if test file exists
        if not Path(suite['file']).exists():
            print(f"\nWARNING: Test file {suite['file']} not found, skipping...")
            results.append({'name': suite['name'], 'status': 'SKIPPED', 'reason': 'File not found'})
            continue
        
        # Build pytest command
        cmd_parts = ['python', '-m', 'pytest', suite['file']]
        if verbose_flag:
            cmd_parts.append(verbose_flag)
        if fast_flag:
            cmd_parts.extend(fast_flag.split())
        
        cmd = ' '.join(cmd_parts)
        
        # Run the test suite
        success = run_command(cmd, f"{suite['name']} - {suite['description']}")
        
        results.append({
            'name': suite['name'],
            'status': 'PASSED' if success else 'FAILED',
            'file': suite['file']
        })
    
    total_elapsed = time.time() - total_start_time
    
    # Print summary
    print("\n" + "="*80)
    print("TEST SUITE SUMMARY")
    print("="*80)
    
    passed_count = sum(1 for r in results if r['status'] == 'PASSED')
    failed_count = sum(1 for r in results if r['status'] == 'FAILED')
    skipped_count = sum(1 for r in results if r['status'] == 'SKIPPED')
    
    for result in results:
        status_icon = "✓" if result['status'] == 'PASSED' else "✗" if result['status'] == 'FAILED' else "⚠"
        print(f"{status_icon} {result['name']}: {result['status']}")
    
    print(f"\nResults: {passed_count} passed, {failed_count} failed, {skipped_count} skipped")
    print(f"Total execution time: {total_elapsed:.2f} seconds")
    
    if failed_count > 0:
        print(f"\n⚠ WARNING: {failed_count} test suite(s) failed!")
        print("Review the output above for details on failures.")
        
        # Provide guidance on common issues
        print("\nCommon troubleshooting steps:")
        print("1. Ensure all required dependencies are installed: pip install -r requirements.txt")
        print("2. Check that test data files are not locked by other processes")
        print("3. Verify sufficient disk space for temporary test files")
        print("4. On Windows, ensure file permissions allow temporary file creation")
        
        return 1
    else:
        print(f"\n🎉 SUCCESS: All {passed_count} test suite(s) passed!")
        print("The real estate direct mail processing system is ready for production.")
        return 0

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)