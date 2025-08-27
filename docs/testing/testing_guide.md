# Comprehensive Spark ETL Testing Framework

## Project Structure (Updated - Streamlined)

```
F:\Data_ETL\spark\
├── src/                                    # Main source code
│   ├── datasets/                          # Dataset generation
│   │   ├── __init__.py
│   │   └── dataset_generator.py           # Enhanced dataset generation with staging workflows
│   ├── spark_classes/                     # Organized Spark classes (12 classes)
│   │   ├── __init__.py
│   │   ├── spark_grouping_strategies.py   # Grouping and aggregation strategies
│   │   ├── spark_memory_optimization.py   # Memory management and optimization
│   │   ├── spark_advanced_transformations.py # Complex data transformations
│   │   ├── spark_query_optimization.py    # Query optimization techniques
│   │   ├── spark_partitioning_strategies.py # Partitioning optimization
│   │   ├── spark_parallelism_optimization.py # Parallelism optimization
│   │   ├── spark_advanced_writing_strategies.py # Advanced writing strategies
│   │   ├── spark_file_formats.py          # File format optimization
│   │   ├── spark_monitoring_debugging.py  # Monitoring and debugging tools
│   │   ├── spark_streaming_etl.py         # Real-time streaming ETL
│   │   ├── spark_sql_connection.py        # Multi-database JDBC connections
│   │   └── fetchsize_usage_examples.py    # JDBC fetchSize optimization
│   ├── sql_server/                        # SQL Server integration
│   │   ├── __init__.py
│   │   └── sql_server_connector.py        # SQL Server connection and operations
│   ├── tests/                             # Streamlined testing framework (3 essential files)
│   │   ├── __init__.py
│   │   ├── master_test_suite.py           # 🎯 MASTER: Consolidated testing framework
│   │   ├── staging_to_curated_etl_tester.py # 🔄 ETL: Specialized ETL workflow testing
│   │   └── test_all_spark_classes.py      # 🧪 PYTEST: Comprehensive pytest-compatible tests
│   └── performance_analysis/              # Performance analysis tools
│       ├── __init__.py
│       ├── performance_analyzer.py        # Base performance analysis
│       └── advanced_performance_tester.py # Configuration testing
├── run_comprehensive_tests.py             # 🚀 MAIN EXECUTION SCRIPT
├── sql_server_config.ini                  # SQL Server configuration
└── docs/                                  # Documentation
    └── testing/                           # Testing documentation
        └── testing_guide.md               # This file
```

## Overview

This comprehensive testing framework is designed to test every function in your Spark ETL project with different data volumes, performance analysis, and SQL Server integration. The framework has been streamlined to use only 3 essential test files that provide complete coverage without redundancy.

### 1. Enhanced Dataset Generation (`src/datasets/`)
- Multiple data types: Sales, Financial, Time Series, Nested data, Customer Master, Product Catalog
- Configurable volumes for ETL environments: Development (10K), Staging (100K), Production-like (1M), Stress Test (5M)
- Data quality issues: 15% realistic quality issues (nulls, negatives, future dates)
- Staging workflows: Complete datasets for staging-to-curated ETL testing

### 2. Organized Spark Classes (`src/spark_classes/`) - 12 Classes Total
- SparkGroupingStrategies: Comprehensive grouping and aggregation optimization
- SparkMemoryOptimizer: Memory management and optimization techniques
- SparkAdvancedTransformations: Complex data transformations and ETL operations
- SparkQueryOptimizer: Query optimization and performance tuning
- SparkPartitioningStrategies: Partitioning optimization techniques
- SparkParallelismOptimizer: Parallelism and task optimization
- SparkAdvancedWritingStrategies: Advanced data writing strategies
- SparkFileFormats: File format optimization
- SparkMonitoringDebugging: Monitoring and debugging tools
- SparkStreamingETL: Real-time streaming ETL
- SparkJDBCConnector: Multi-database JDBC connections
- FetchSizeOptimizer: JDBC fetchSize optimization

### 3. SQL Server Integration (`src/sql_server/`)
- Configuration management: Centralized SQL Server configuration
- Performance monitoring: Built-in timing and throughput measurement
- Bulk operations: Optimized read/write operations for large datasets
- ETL workflows: Staging-to-curated database workflows
- Note: SparkJDBCConnector auto-downloads drivers via spark.jars.packages; SQLServerConnector expects a local mssql-jdbc JAR.

### 4. Streamlined Testing Framework (`src/tests/`) - 3 Essential Files

#### 🎯 `master_test_suite.py` (Master Testing Framework)
- Consolidated testing class: MasterSparkETLTester
- Volume-based testing: Tests functions with different data volumes
- Performance measurement: Execution time, memory usage, throughput
- All 12 Spark classes: Comprehensive testing of every function
- SQL Server integration: Database read/write testing
- ETL workflow integration: Complete pipeline testing

#### 🔄 `staging_to_curated_etl_tester.py` (Specialized ETL Testing)
- Complete ETL pipeline: Staging-to-curated data workflows
- Data quality analysis: Comprehensive quality metrics and reporting
- Data cleaning: Configurable cleaning rules and business logic
- Complex joins: Multi-table joins and aggregations
- Performance across configurations: 5 different ETL configurations tested
- SQL Server workflows: Database staging and curated table operations

#### 🧪 `test_all_spark_classes.py` (Pytest-Compatible Testing)
- Pytest fixtures: Professional pytest-style testing
- All Spark classes: Comprehensive validation of every class and method
- Configuration comparison: Advanced performance testing across configurations
- Enhanced dataset testing: Validation of new dataset generation capabilities
- Integration testing: End-to-end testing with SQL Server

### 5. Performance Analysis (`src/performance_analysis/`)
- Configuration testing: 6 different Spark configurations
- Execution plan analysis: Identify optimization opportunities
- Memory profiling: Track memory usage patterns
- Scalability analysis: Understand how operations scale
- Visualization: Generate performance charts and reports

## Quick Start Guide

### 1. Setup Requirements
```bash
pip install pyspark pytest psutil matplotlib pandas configparser
# Download SQL Server JDBC driver (mssql-jdbc-12.4.2.jre8.jar) if using SQLServerConnector
```

### 2. Configure SQL Server
Edit `sql_server_config.ini` with your SQL Server details:
```ini
[DEFAULT]
server = your_sql_server
database = your_database
username = your_username
password = your_password
```

### 3. Run Comprehensive Tests (Main Execution Script)
```bash
# 🎯 Run ALL tests comprehensively (recommended)
python run_comprehensive_tests.py --mode all

# 🔧 Test specific components
python run_comprehensive_tests.py --mode spark-classes     # Test all 12 Spark classes
python run_comprehensive_tests.py --mode performance       # Configuration testing
python run_comprehensive_tests.py --mode etl-workflow      # ETL pipeline testing
python run_comprehensive_tests.py --mode sql-integration   # SQL Server testing
python run_comprehensive_tests.py --mode pytest            # Run pytest suite via runner

# 📊 Different environments
python run_comprehensive_tests.py --mode all --environment staging
python run_comprehensive_tests.py --mode all --environment production_like

# ⚙️ Specific configurations
python run_comprehensive_tests.py --mode performance \
    --configurations adaptive_optimized memory_optimized join_optimized

# 🗄️ Skip SQL Server if not available
python run_comprehensive_tests.py --mode all --no-sql-server
```

### 4. Run Individual Test Files
```bash
# Master test suite (consolidated testing)
python -c "from src.tests.master_test_suite import MasterSparkETLTester; tester = MasterSparkETLTester(); tester.run_all_tests()"

# Pytest-compatible tests
cd src/tests
pytest test_all_spark_classes.py -v

# ETL workflow testing
python -c "from src.tests.staging_to_curated_etl_tester import StagingToCuratedETL; etl = StagingToCuratedETL(); etl.run_comprehensive_etl_with_performance_analysis()"
```

## Testing Framework Details

### Master Test Suite (`master_test_suite.py`)
Primary testing framework that consolidates all functionality:

```python
from src.tests.master_test_suite import MasterSparkETLTester

# Initialize master tester
tester = MasterSparkETLTester(use_sql_server=True)

# Test all Spark classes
tester.test_all_spark_classes()

# Test SQL Server integration
tester.test_sql_server_integration()

# Test ETL workflows
tester.test_etl_workflow()

# Generate performance report
tester.generate_performance_report()

# Run everything
tester.run_all_tests()
```

### ETL Workflow Testing (`staging_to_curated_etl_tester.py`)
Specialized testing for complete ETL pipelines:

```python
from src.tests.staging_to_curated_etl_tester import StagingToCuratedETL

# Initialize ETL tester
etl = StagingToCuratedETL()

# Run comprehensive ETL testing with performance analysis
results, report = etl.run_comprehensive_etl_with_performance_analysis(
    environment="staging"
)
```

### Pytest Testing (`test_all_spark_classes.py`)
Professional pytest-style testing:

```bash
# Run all pytest tests
pytest src/tests/test_all_spark_classes.py -v

# Run specific test categories
pytest src/tests/test_all_spark_classes.py::TestAllSparkClasses::test_grouping_strategies_all_functions -v
pytest src/tests/test_all_spark_classes.py::TestAllSparkClasses::test_staging_to_curated_etl_workflow -v
pytest src/tests/test_all_spark_classes.py::TestAllSparkClasses::test_advanced_performance_configurations -v
```

## Testing Scenarios

### 1. Function Testing with Multiple Volumes
Each function is tested with dataset sizes defined in `DatasetGenerator.get_dataset_configs()`:
- Small (1,000 records): Quick validation testing
- Medium (50,000 records): Integration/performance testing
- Large (500,000 records): Stress testing
- XLarge (2,000,000 records): Scalability testing (optional where applicable)

### 2. Performance Metrics Measured
- Execution time: Milliseconds for operation completion
- Memory usage: Memory delta during operation
- Throughput: Records processed per second
- SQL Server I/O: Read/write performance to database
- Configuration comparison: Best performing Spark settings

### 3. Spark Configuration Testing
The framework tests 6 different Spark configurations:
- Default: Baseline configuration
- Adaptive Optimized: Adaptive query execution enabled
- Memory Optimized: Memory-intensive workload optimization
- Large Data Optimized: Very large dataset optimization
- Join Optimized: Complex join and skew handling
- I/O Optimized: File operations and compression optimization

### 4. ETL Workflow Testing
- Staging data generation: Realistic datasets with quality issues
- Data quality analysis: Comprehensive quality metrics
- Data cleaning: Configurable cleaning rules
- Complex joins: Multi-table dimensional modeling
- Performance measurement: End-to-end pipeline performance

### 5. SQL Server Integration Testing
- Write performance: Bulk insert operations with different batch sizes
- Read performance: Parallel reading with configurable partitions
- Query performance: Complex aggregations and filtering
- Connection optimization: Connection pooling and timeout settings

## Generated Reports

### 1. Performance Reports
The framework generates comprehensive performance reports:
- Master Test Report: Complete results for all functions tested
- Configuration Comparison Report: Best performing configurations
- ETL Performance Report: End-to-end pipeline analysis
- Scaling Analysis: How operations scale with data size

### 2. Report Files Generated
- `master_test_report_YYYYMMDD_HHMMSS.json`
- `configuration_performance_report_YYYYMMDD_HHMMSS.json`
- `etl_performance_report_YYYYMMDD_HHMMSS.json`

## Advanced Features

### 1. Data Quality Testing
- Realistic quality issues: 15% of data contains quality problems
- Configurable cleaning: Business rules for different table types
- Quality metrics: Comprehensive analysis and reporting

### 2. Configuration Optimization
- Automatic testing: All configurations tested automatically
- Performance comparison: Best configuration identified per operation
- Recommendations: Specific optimization suggestions

### 3. Scaling Analysis
- Linear vs superlinear: Identifies scaling patterns
- Bottleneck detection: Pinpoints performance limitations
- Capacity planning: Recommendations for production sizing

## Best Practices for Testing

### 1. Test Execution Order
1. Start with spark-classes mode: Validate all individual functions
2. Run performance mode: Identify optimal configurations
3. Test etl-workflow: Validate complete pipelines
4. Run sql-integration: Test database operations

### 2. Environment Usage
- Development: Quick validation and debugging
- Staging: Integration testing and validation
- Production-like: Performance testing and optimization
- Stress-test: Capacity planning and limits testing

### 3. Configuration Testing
- Test all configurations: Each has different strengths
- Monitor memory: Large datasets require memory optimization
- Review reports: Use recommendations for production settings

This streamlined testing framework provides comprehensive coverage of all Spark ETL functionality while maintaining a clean, organized structure with only the essential test files.
