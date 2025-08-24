# Comprehensive Spark ETL Testing Framework

A streamlined, comprehensive testing framework for Apache Spark ETL operations with advanced performance analysis, SQL Server integration, and staging-to-curated data workflows.

## 🎯 **Framework Overview**

This framework provides complete testing coverage for Spark ETL operations including:
- **12 Spark Classes** with **70+ functions** tested across multiple configurations
- **Performance analysis** with 6 different Spark configurations
- **Staging-to-curated ETL workflows** with data quality validation
- **SQL Server integration** with bulk operations and performance benchmarking
- **Realistic dataset generation** with configurable data quality issues

## 📁 **Project Structure (Streamlined)**

```
F:\Data_ETL\spark\
├── 📄 README.md                           # This comprehensive guide
├── 📄 requirements.txt                    # Python dependencies
├── 📄 sql_server_config.ini              # SQL Server configuration
├── 🚀 run_comprehensive_tests.py          # MAIN EXECUTION SCRIPT
│
├── 📂 src/                               # All source code (organized)
│   ├── 📂 spark_classes/                # Spark ETL classes (12 classes)
│   │   ├── spark_grouping_strategies.py
│   │   ├── spark_memory_optimization.py
│   │   ├── spark_advanced_transformations.py
│   │   ├── spark_query_optimization.py
│   │   ├── spark_partitioning_strategies.py
│   │   ├── spark_parallelism_optimization.py
│   │   ├── spark_advanced_writing_strategies.py
│   │   ├── spark_file_formats.py
│   │   ├── spark_monitoring_debugging.py
│   │   ├── spark_streaming_etl.py
│   │   ├── spark_sql_connection.py
│   │   └── fetchsize_usage_examples.py
│   │
│   ├── 📂 datasets/                      # Dataset generation
│   │   └── dataset_generator.py          # Enhanced with staging datasets
│   │
│   ├── 📂 tests/                         # Consolidated testing framework
│   │   ├── 🎯 master_test_suite.py       # MASTER TEST SUITE (consolidated)
│   │   ├── staging_to_curated_etl_tester.py # ETL workflow testing
│   │   └── test_all_spark_classes.py     # Comprehensive class testing
│   │
│   ├── 📂 performance_analysis/          # Performance testing tools
│   │   ├── performance_analyzer.py       # Base performance analysis
│   │   └── advanced_performance_tester.py # Configuration testing
│   │
│   └── 📂 sql_server/                    # SQL Server integration
│       └── sql_server_connector.py       # Connection and operations
│
└── 📂 docs/                              # Documentation
    ├── testing/
    │   └── testing_guide.md              # Detailed testing guide
    └── [spark_class_docs].md             # Individual class documentation
```

## 🚀 **Quick Start Guide**

### **Prerequisites**
```bash
# Install dependencies
pip install -r requirements.txt

# Configure SQL Server (optional)
# Edit sql_server_config.ini with your SQL Server details
```

### **Main Execution Options**

The framework provides a single main script with multiple testing modes:

```bash
# 🎯 Run ALL tests comprehensively (recommended)
python run_comprehensive_tests.py --mode all

# 🔧 Test specific components
python run_comprehensive_tests.py --mode spark-classes
python run_comprehensive_tests.py --mode performance
python run_comprehensive_tests.py --mode etl-workflow
python run_comprehensive_tests.py --mode sql-integration

# 📊 Test with different environments
python run_comprehensive_tests.py --mode all --environment staging
python run_comprehensive_tests.py --mode all --environment production_like

# ⚙️ Test specific Spark configurations
python run_comprehensive_tests.py --mode performance \
    --configurations adaptive_optimized memory_optimized join_optimized

# 🗄️ Skip SQL Server if not available
python run_comprehensive_tests.py --mode all --no-sql-server
```

## 🧪 **Testing Modes Explained**

### **1. `--mode all` (Comprehensive Testing)**
- Tests all 12 Spark classes with 70+ functions
- Performance analysis across multiple configurations
- Complete staging-to-curated ETL workflow
- SQL Server integration testing
- Generates comprehensive performance reports

### **2. `--mode spark-classes` (Class Testing)**
- Tests individual Spark class methods
- Volume-based testing (small, medium, large datasets)
- Memory usage and performance measurement
- Function-specific validation

### **3. `--mode performance` (Configuration Testing)**
- Tests 6 different Spark configurations:
  - `default` - Baseline performance
  - `adaptive_optimized` - Adaptive query execution
  - `memory_optimized` - Memory-intensive workloads
  - `large_data_optimized` - Very large datasets
  - `join_optimized` - Complex joins and skewed data
  - `io_optimized` - File operations and compression

### **4. `--mode etl-workflow` (ETL Pipeline Testing)**
- Complete staging-to-curated data pipeline
- Data quality analysis and cleaning
- Complex joins and aggregations
- Performance measurement across ETL stages

### **5. `--mode sql-integration` (Database Testing)**
- SQL Server bulk operations
- Read/write performance benchmarking
- Connection optimization testing
- Spark vs SQL Server performance comparison

## 📊 **Dataset Environments**

The framework supports multiple environment configurations:

| Environment | Customers | Products | Stores | Transactions | Use Case |
|-------------|-----------|----------|--------|--------------|----------|
| `development` | 1,000 | 500 | 20 | 10,000 | Unit testing |
| `staging` | 10,000 | 2,000 | 100 | 100,000 | Integration testing |
| `production_like` | 100,000 | 10,000 | 500 | 1,000,000 | Performance testing |
| `stress_test` | 500,000 | 50,000 | 1,000 | 5,000,000 | Stress testing |

## 🔧 **Configuration Details**

### **Spark Configurations Tested**

1. **Default Configuration**
   - Baseline Spark settings
   - No specific optimizations

2. **Adaptive Optimized**
   ```
   spark.sql.adaptive.enabled=true
   spark.sql.adaptive.coalescePartitions.enabled=true
   spark.sql.adaptive.skewJoin.enabled=true
   ```

3. **Memory Optimized**
   ```
   spark.executor.memory=4g
   spark.executor.memoryFraction=0.8
   spark.sql.execution.arrow.maxRecordsPerBatch=10000
   ```

4. **Large Data Optimized**
   ```
   spark.sql.files.maxPartitionBytes=268435456
   spark.sql.shuffle.partitions=400
   ```

5. **Join Optimized**
   ```
   spark.sql.adaptive.skewJoin.enabled=true
   spark.sql.autoBroadcastJoinThreshold=104857600
   ```

6. **I/O Optimized**
   ```
   spark.sql.parquet.compression.codec=snappy
   spark.serializer=org.apache.spark.serializer.KryoSerializer
   ```

## 📈 **Performance Metrics Measured**

For every test, the framework measures:
- ⏱️ **Execution Time** (milliseconds)
- 💾 **Memory Usage** (MB delta)
- 🚀 **Throughput** (records/second)
- 📊 **Scalability** (linear vs superlinear)
- 🔄 **SQL Server I/O** (read/write performance)

## 🗄️ **SQL Server Integration**

### **Configuration**
Edit `sql_server_config.ini`:
```ini
[DEFAULT]
server = your_sql_server
database = your_database
username = your_username
password = your_password
```

### **Capabilities**
- **Bulk Operations**: Optimized read/write with configurable batch sizes
- **Performance Benchmarking**: Measures throughput for different data volumes
- **Staging-to-Curated Workflows**: Complete ETL pipeline testing
- **Connection Optimization**: Tests different connection strategies

## 📋 **Generated Reports**

The framework generates comprehensive reports:

1. **Master Test Report** (`master_test_report_YYYYMMDD_HHMMSS.json`)
   - Complete test results for all functions
   - Performance metrics and trends
   - Configuration recommendations

2. **Configuration Comparison Report** (`configuration_performance_report_YYYYMMDD_HHMMSS.json`)
   - Best performing configurations for each operation
   - Optimization recommendations
   - Scaling analysis

3. **ETL Performance Report** (`etl_performance_report_YYYYMMDD_HHMMSS.json`)
   - End-to-end pipeline performance
   - Data quality metrics
   - Bottleneck identification

## 🎯 **Key Features**

### **✅ Comprehensive Coverage**
- **Every function** in **every Spark class** tested
- **Multiple data volumes** (1K to 5M+ records)
- **Realistic data scenarios** with quality issues

### **✅ Performance Optimization**
- **Automatic configuration testing** finds optimal settings
- **Scaling analysis** shows performance characteristics
- **Memory and throughput optimization** recommendations

### **✅ Real-World Scenarios**
- **Staging-to-curated workflows** with data cleaning
- **SQL Server integration** with bulk operations
- **Data quality issues** simulation for robust ETL testing

### **✅ Advanced Reporting**
- **Detailed performance metrics** for every operation
- **Configuration recommendations** based on actual data
- **Executive summaries** for stakeholder communication

## 🔄 **ETL Workflow Testing**

The framework includes complete ETL pipeline testing:

### **Pipeline Stages**
1. **Data Generation** - Realistic datasets with quality issues
2. **Staging Load** - Bulk insert to SQL Server staging tables
3. **Data Quality Analysis** - Comprehensive quality metrics
4. **Data Cleaning** - Configurable cleaning rules
5. **Complex Joins** - Create curated dimensional model
6. **Curated Load** - Optimized writes to curated tables

### **Data Quality Features**
- **15% quality issues** introduced (nulls, negatives, future dates)
- **Configurable cleaning rules** per table type
- **Quality metrics reporting** with recommendations

## 📚 **Advanced Usage Examples**

### **Custom Configuration Testing**
```bash
# Test specific configurations with large datasets
python run_comprehensive_tests.py \
    --mode performance \
    --environment production_like \
    --configurations memory_optimized join_optimized
```

### **ETL Workflow with Quality Analysis**
```bash
# Test complete ETL pipeline with data quality checks
python run_comprehensive_tests.py \
    --mode etl-workflow \
    --environment staging
```

### **SQL Server Performance Benchmarking**
```bash
# Benchmark SQL Server operations across environments
python run_comprehensive_tests.py \
    --mode sql-integration \
    --environment production_like
```

## 🛠️ **Development and Extension**

### **Adding New Spark Classes**
1. Create class in `src/spark_classes/`
2. Add test method to `master_test_suite.py`
3. Update the `test_all_spark_classes()` method

### **Adding New Configurations**
1. Update `get_spark_configurations()` in `advanced_performance_tester.py`
2. Add configuration to command-line choices

### **Customizing ETL Workflows**
1. Modify cleaning rules in `staging_to_curated_etl_tester.py`
2. Add new data quality checks
3. Extend join logic for specific business requirements

## 🏆 **Best Practices**

1. **Start with small environments** for development and testing
2. **Use staging environment** for integration testing
3. **Run production_like tests** before production deployment
4. **Monitor memory usage** during large dataset testing
5. **Review performance reports** for optimization opportunities
6. **Test with realistic data quality issues** for robust ETL

## 📞 **Support and Documentation**

- **Detailed Testing Guide**: `docs/testing/testing_guide.md`
- **Class Documentation**: Individual `.md` files in `docs/`
- **Performance Reports**: Generated in JSON format with human-readable summaries

---

## 🎉 **Quick Test Execution**

Ready to start? Run this command for a complete test:

```bash
python run_comprehensive_tests.py --mode all --environment staging
```

This will test all 12 Spark classes, 70+ functions, multiple configurations, ETL workflows, and generate comprehensive performance reports!

---

*This framework provides complete visibility into Spark ETL performance across all configurations and realistic data scenarios, with automated optimization recommendations.*
