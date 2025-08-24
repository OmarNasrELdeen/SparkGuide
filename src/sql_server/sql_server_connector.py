"""
SQL Server Configuration and Connection Management
Handles all SQL Server connections and configurations for testing
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import configparser
import time

class SQLServerConfig:
    def __init__(self, config_file="sql_server_config.ini"):
        """Initialize SQL Server configuration"""
        self.config_file = config_file
        self.config = configparser.ConfigParser()
        self.load_config()

    def load_config(self):
        """Load configuration from file or create default"""
        if os.path.exists(self.config_file):
            self.config.read(self.config_file)
        else:
            self.create_default_config()

    def create_default_config(self):
        """Create default configuration file"""
        self.config['DEFAULT'] = {
            'server': 'localhost',
            'port': '1433',
            'database': 'spark_test_db',
            'username': 'spark_user',
            'password': 'your_password',
            'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
            'trustServerCertificate': 'true',
            'encrypt': 'false'
        }

        self.config['PERFORMANCE'] = {
            'fetchsize': '10000',
            'batchsize': '10000',
            'numPartitions': '8',
            'connectionTimeout': '30',
            'queryTimeout': '300'
        }

        self.config['TEST_TABLES'] = {
            'sales_small': 'test_sales_small',
            'sales_medium': 'test_sales_medium',
            'sales_large': 'test_sales_large',
            'financial': 'test_financial',
            'time_series': 'test_time_series'
        }

        with open(self.config_file, 'w') as f:
            self.config.write(f)

        print(f"Created default config file: {self.config_file}")
        print("Please update the configuration with your SQL Server details")

    def get_jdbc_url(self):
        """Get JDBC URL for SQL Server connection"""
        server = self.config['DEFAULT']['server']
        port = self.config['DEFAULT']['port']
        database = self.config['DEFAULT']['database']
        trust_cert = self.config['DEFAULT']['trustServerCertificate']
        encrypt = self.config['DEFAULT']['encrypt']

        return f"jdbc:sqlserver://{server}:{port};databaseName={database};trustServerCertificate={trust_cert};encrypt={encrypt}"

    def get_connection_properties(self):
        """Get connection properties for JDBC"""
        return {
            "user": self.config['DEFAULT']['username'],
            "password": self.config['DEFAULT']['password'],
            "driver": self.config['DEFAULT']['driver'],
            "fetchsize": self.config['PERFORMANCE']['fetchsize'],
            "batchsize": self.config['PERFORMANCE']['batchsize']
        }

class SQLServerConnector:
    def __init__(self, config_file="sql_server_config.ini"):
        """Initialize SQL Server connector"""
        self.config = SQLServerConfig(config_file)
        self.spark = None
        self.jdbc_url = self.config.get_jdbc_url()
        self.properties = self.config.get_connection_properties()

    def initialize_spark(self, app_name="SQLServerTesting"):
        """Initialize Spark session with SQL Server JDBC driver"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.jars", "mssql-jdbc-12.4.2.jre8.jar") \
            .getOrCreate()
        return self.spark

    def test_connection(self):
        """Test SQL Server connection"""
        try:
            if not self.spark:
                self.initialize_spark()

            # Try to read from a system table
            test_df = self.spark.read \
                .jdbc(self.jdbc_url, "(SELECT 1 as test_connection) as test", properties=self.properties)

            result = test_df.collect()
            if result and result[0]['test_connection'] == 1:
                print("✅ SQL Server connection successful!")
                return True
            else:
                print("❌ SQL Server connection failed!")
                return False
        except Exception as e:
            print(f"❌ SQL Server connection error: {str(e)}")
            return False

    def create_test_tables(self):
        """Create test tables in SQL Server"""
        tables_sql = {
            'test_sales_small': """
                CREATE TABLE test_sales_small (
                    id INT PRIMARY KEY,
                    category NVARCHAR(50),
                    region NVARCHAR(50),
                    product_name NVARCHAR(100),
                    sales INT,
                    date DATE,
                    status NVARCHAR(20),
                    customer_id NVARCHAR(50),
                    quantity INT
                )
            """,
            'test_performance_metrics': """
                CREATE TABLE test_performance_metrics (
                    test_id NVARCHAR(100),
                    test_name NVARCHAR(200),
                    dataset_size INT,
                    operation_type NVARCHAR(50),
                    execution_time_ms BIGINT,
                    memory_usage_mb BIGINT,
                    shuffle_read_mb BIGINT,
                    shuffle_write_mb BIGINT,
                    test_timestamp DATETIME2,
                    spark_config NVARCHAR(MAX)
                )
            """
        }

        print("Creating test tables in SQL Server...")
        for table_name, sql in tables_sql.items():
            try:
                # Note: Direct DDL execution requires additional setup
                # This is a template - actual implementation would use SQL Server tools
                print(f"Table schema for {table_name}:")
                print(sql)
                print()
            except Exception as e:
                print(f"Error creating table {table_name}: {str(e)}")

    def write_dataframe(self, df, table_name, mode="overwrite"):
        """Write DataFrame to SQL Server with performance monitoring"""
        start_time = time.time()

        try:
            df.write \
                .jdbc(self.jdbc_url, table_name, mode=mode, properties=self.properties)

            execution_time = (time.time() - start_time) * 1000
            record_count = df.count()

            print(f"✅ Successfully wrote {record_count:,} records to {table_name}")
            print(f"   Execution time: {execution_time:.2f} ms")
            print(f"   Throughput: {record_count / (execution_time / 1000):.2f} records/sec")

            return True, execution_time, record_count

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            print(f"❌ Error writing to {table_name}: {str(e)}")
            return False, execution_time, 0

    def read_dataframe(self, table_name, num_partitions=None):
        """Read DataFrame from SQL Server with performance monitoring"""
        start_time = time.time()

        try:
            read_options = self.properties.copy()
            if num_partitions:
                read_options["numPartitions"] = str(num_partitions)

            df = self.spark.read \
                .jdbc(self.jdbc_url, table_name, properties=read_options)

            # Trigger action to measure actual read time
            record_count = df.count()
            execution_time = (time.time() - start_time) * 1000

            print(f"✅ Successfully read {record_count:,} records from {table_name}")
            print(f"   Execution time: {execution_time:.2f} ms")
            print(f"   Throughput: {record_count / (execution_time / 1000):.2f} records/sec")

            return df, execution_time, record_count

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            print(f"❌ Error reading from {table_name}: {str(e)}")
            return None, execution_time, 0

    def execute_sql_query(self, query, description=""):
        """Execute SQL query and return results with timing"""
        start_time = time.time()

        try:
            df = self.spark.read \
                .jdbc(self.jdbc_url, f"({query}) as subquery", properties=self.properties)

            result = df.collect()
            execution_time = (time.time() - start_time) * 1000

            print(f"✅ Query executed successfully: {description}")
            print(f"   Execution time: {execution_time:.2f} ms")
            print(f"   Rows returned: {len(result)}")

            return result, execution_time

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            print(f"❌ Query execution error: {str(e)}")
            return None, execution_time

    def benchmark_operations(self, df, table_name):
        """Benchmark various operations with the dataset"""
        results = {}

        print(f"\n=== Benchmarking operations for {table_name} ===")

        # Benchmark write operation
        print("1. Testing WRITE operation...")
        success, write_time, write_count = self.write_dataframe(df, table_name)
        results['write'] = {'time': write_time, 'records': write_count, 'success': success}

        if success:
            # Benchmark read operation
            print("2. Testing READ operation...")
            read_df, read_time, read_count = self.read_dataframe(table_name)
            results['read'] = {'time': read_time, 'records': read_count, 'success': read_df is not None}

            # Benchmark aggregation query
            print("3. Testing AGGREGATION query...")
            agg_query = f"""
                SELECT category, region, 
                       COUNT(*) as record_count,
                       SUM(sales) as total_sales,
                       AVG(sales) as avg_sales
                FROM {table_name}
                GROUP BY category, region
            """
            agg_result, agg_time = self.execute_sql_query(agg_query, "Aggregation query")
            results['aggregation'] = {'time': agg_time, 'success': agg_result is not None}

            # Benchmark filtering query
            print("4. Testing FILTERING query...")
            filter_query = f"""
                SELECT * FROM {table_name}
                WHERE sales > 5000 AND status = 'completed'
                ORDER BY sales DESC
            """
            filter_result, filter_time = self.execute_sql_query(filter_query, "Filtering query")
            results['filtering'] = {'time': filter_time, 'success': filter_result is not None}

        return results

# Example usage
if __name__ == "__main__":
    # Initialize connector
    connector = SQLServerConnector()

    # Test connection
    if connector.test_connection():
        # Create sample data for testing
        from dataset_generator import DatasetGenerator

        generator = DatasetGenerator(connector.spark)
        test_df = generator.generate_sales_data(10000)

        # Benchmark operations
        results = connector.benchmark_operations(test_df, "test_sales_benchmark")

        print("\n=== Benchmark Results ===")
        for operation, metrics in results.items():
            print(f"{operation.upper()}: {metrics}")
