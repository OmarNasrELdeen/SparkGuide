"""
Streamlined Main Execution Script for Comprehensive Spark ETL Testing
Consolidated script that orchestrates all testing using the master test suite
"""

import argparse
import sys
import os
from datetime import datetime

# Add src to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from tests.master_test_suite import MasterSparkETLTester
from tests.staging_to_curated_etl_tester import StagingToCuratedETL
from performance_analysis.advanced_performance_tester import AdvancedPerformanceTester

def main():
    """Main execution function for comprehensive testing"""
    parser = argparse.ArgumentParser(description='Comprehensive Spark ETL Testing Framework')
    parser.add_argument('--mode',
                       choices=['all', 'spark-classes', 'performance', 'etl-workflow', 'sql-integration', 'pytest'],
                       default='all',
                       help='Testing mode to run')
    parser.add_argument('--environment',
                       choices=['development', 'staging', 'production_like', 'stress_test'],
                       default='staging',
                       help='Dataset environment size')
    parser.add_argument('--configurations', nargs='+',
                       choices=['default', 'adaptive_optimized', 'memory_optimized', 'large_data_optimized', 'join_optimized', 'io_optimized'],
                       default=['default', 'adaptive_optimized', 'memory_optimized'],
                       help='Spark configurations to test')
    parser.add_argument('--no-sql-server', action='store_true',
                       help='Skip SQL Server integration tests')
    parser.add_argument('--output-dir', default='test_results',
                       help='Output directory for results')

    args = parser.parse_args()

    print("🚀 COMPREHENSIVE SPARK ETL TESTING FRAMEWORK")
    print("="*80)
    print(f"Mode: {args.mode}")
    print(f"Environment: {args.environment}")
    print(f"Configurations: {args.configurations}")
    print(f"SQL Server: {'Disabled' if args.no_sql_server else 'Enabled'}")
    print(f"Output Directory: {args.output_dir}")
    print("="*80)

    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)

    try:
        if args.mode == 'all':
            run_comprehensive_testing(args)
        elif args.mode == 'spark-classes':
            run_spark_classes_testing(args)
        elif args.mode == 'performance':
            run_performance_testing(args)
        elif args.mode == 'etl-workflow':
            run_etl_workflow_testing(args)
        elif args.mode == 'sql-integration':
            run_sql_integration_testing(args)
        elif args.mode == 'pytest':
            run_pytest_testing(args)

        print("\n✅ Testing completed successfully!")

    except Exception as e:
        print(f"\n❌ Testing failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def run_comprehensive_testing(args):
    """Run all testing components comprehensively"""
    print("\n" + "="*80)
    print("RUNNING COMPREHENSIVE TESTING - ALL COMPONENTS")
    print("="*80)

    # Initialize master tester
    tester = MasterSparkETLTester(use_sql_server=not args.no_sql_server)

    # Run all tests
    tester.run_all_tests(include_etl_workflow=True)

    # Run advanced performance testing
    if args.mode == 'all':
        print("\n" + "="*60)
        print("ADVANCED PERFORMANCE CONFIGURATION TESTING")
        print("="*60)

        perf_tester = AdvancedPerformanceTester()
        config_results = perf_tester.test_all_classes_with_configurations(['small', 'medium'])
        comparison_report = perf_tester.generate_configuration_comparison_report()

def run_spark_classes_testing(args):
    """Test all Spark classes with their methods"""
    print("\n" + "="*60)
    print("SPARK CLASSES TESTING")
    print("="*60)

    tester = MasterSparkETLTester(use_sql_server=not args.no_sql_server)
    tester.test_all_spark_classes()
    tester.generate_performance_report()

def run_performance_testing(args):
    """Test performance across different Spark configurations"""
    print("\n" + "="*60)
    print("PERFORMANCE CONFIGURATION TESTING")
    print("="*60)

    tester = AdvancedPerformanceTester()

    # Filter configurations based on arguments
    all_configurations = tester.get_spark_configurations()
    selected_configs = {k: v for k, v in all_configurations.items() if k in args.configurations}

    print(f"Testing {len(selected_configs)} configurations: {list(selected_configs.keys())}")

    # Override configurations
    tester.get_spark_configurations = lambda: selected_configs

    # Run performance tests
    dataset_sizes = ['small', 'medium'] if args.environment in ['development', 'staging'] else ['small', 'medium', 'large']
    config_results = tester.test_all_classes_with_configurations(dataset_sizes)
    comparison_report = tester.generate_configuration_comparison_report()

def run_etl_workflow_testing(args):
    """Test complete staging-to-curated ETL workflow"""
    print("\n" + "="*60)
    print("STAGING-TO-CURATED ETL WORKFLOW TESTING")
    print("="*60)

    # Initialize ETL tester
    etl = StagingToCuratedETL()

    # Run ETL workflow testing
    etl_results, etl_report = etl.run_comprehensive_etl_with_performance_analysis(
        environment=args.environment
    )

    print(f"\n✅ ETL workflow testing completed")

def run_sql_integration_testing(args):
    """Test SQL Server integration comprehensively"""
    print("\n" + "="*60)
    print("SQL SERVER INTEGRATION TESTING")
    print("="*60)

    if args.no_sql_server:
        print("⚠️  SQL Server testing skipped (--no-sql-server flag)")
        return

    tester = MasterSparkETLTester(use_sql_server=True)
    tester.test_sql_server_integration()
    print(f"\n✅ SQL Server integration testing completed")

def run_pytest_testing(args):
    """Run pytest for unit and integration tests"""
    print("\n" + "="*60)
    print("PYTEST UNIT AND INTEGRATION TESTING")
    print("="*60)

    # Discover and run all pytest tests
    import pytest

    # Set pytest options
    pytest_args = [
        '--tb=short',  # Shorter tracebacks
        '-q',          # Quiet mode, less output
        '--disable-warnings',  # Disable warnings
        f'--junitxml={os.path.join(args.output_dir, "pytest_results.xml")}'  # JUnit XML output
    ]

    # Run pytest
    exit_code = pytest.main(pytest_args)

    if exit_code == 0:
        print("✅ Pytest completed successfully")
    else:
        print(f"❌ Pytest failed with exit code {exit_code}")
        sys.exit(exit_code)

if __name__ == "__main__":
    main()
