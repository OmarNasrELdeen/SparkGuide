"""
Spark JDBC Connection and Advanced Data Source Integration
This module demonstrates comprehensive JDBC connections with all optimization options
including partitioning, bounds, and multi-database support
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

class SparkJDBCConnector:
    def __init__(self, app_name="SparkJDBCETL"):
        """Initialize Spark session with JDBC optimizations"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages",
                   "com.microsoft.sqlserver:mssql-jdbc:11.2.3.jre8,"
                   "mysql:mysql-connector-java:8.0.33,"
                   "org.postgresql:postgresql:42.6.0,"
                   "com.oracle.database.jdbc:ojdbc8:21.9.0.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        # Database-specific configurations
        self.db_configs = {
            "sqlserver": {
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "port": 1433,
                "url_format": "jdbc:sqlserver://{server}:{port};databaseName={database}"
            },
            "mysql": {
                "driver": "com.mysql.cj.jdbc.Driver",
                "port": 3306,
                "url_format": "jdbc:mysql://{server}:{port}/{database}"
            },
            "postgresql": {
                "driver": "org.postgresql.Driver",
                "port": 5432,
                "url_format": "jdbc:postgresql://{server}:{port}/{database}"
            },
            "oracle": {
                "driver": "oracle.jdbc.driver.OracleDriver",
                "port": 1521,
                "url_format": "jdbc:oracle:thin:@{server}:{port}:{database}"
            }
        }

    def get_optimized_connection_properties(self, db_type, username, password, **kwargs):
        """Get optimized connection properties for different databases"""
        base_properties = {
            "user": username,
            "password": password,
            "driver": self.db_configs[db_type]["driver"]
        }

        # Extract fetchSize if provided and add as universal JDBC property
        fetchsize = kwargs.pop("fetchsize", None)
        if fetchsize:
            base_properties["fetchsize"] = str(fetchsize)

        # Database-specific optimizations
        if db_type == "sqlserver":
            base_properties.update({
                "encrypt": "true",
                "trustServerCertificate": "true",
                "loginTimeout": "30",
                "socketTimeout": "0",
                "selectMethod": "cursor",
                "sendStringParametersAsUnicode": "false",
                "prepareThreshold": "3",
                "packetSize": "8192"
            })
        elif db_type == "mysql":
            base_properties.update({
                "useSSL": "false",
                "allowPublicKeyRetrieval": "true",
                "useUnicode": "true",
                "characterEncoding": "utf8",
                "autoReconnect": "true",
                "cachePrepStmts": "true",
                "prepStmtCacheSize": "250",
                "prepStmtCacheSqlLimit": "2048",
                "useServerPrepStmts": "true"
            })
            # For MySQL, also set defaultFetchSize if fetchsize is provided
            if fetchsize:
                base_properties["defaultFetchSize"] = str(fetchsize)

        elif db_type == "postgresql":
            base_properties.update({
                "ssl": "false",
                "prepareThreshold": "5",
                "preparedStatementCacheQueries": "256",
                "preparedStatementCacheSizeMiB": "5",
                "defaultRowFetchSize": "10000"
            })
            # Override defaultRowFetchSize if fetchsize is provided
            if fetchsize:
                base_properties["defaultRowFetchSize"] = str(fetchsize)

        elif db_type == "oracle":
            base_properties.update({
                "oracle.jdbc.ReadTimeout": "0",
                "oracle.net.CONNECT_TIMEOUT": "10000",
                "oracle.jdbc.defaultRowPrefetch": "20",
                "useFetchSizeWithLongColumn": "true"
            })
            # Override defaultRowPrefetch if fetchsize is provided
            if fetchsize:
                base_properties["oracle.jdbc.defaultRowPrefetch"] = str(min(50, fetchsize // 100))

        # Add custom properties
        base_properties.update(kwargs)
        return base_properties

    def extract_with_partitioning(self, db_type, server, database, username, password,
                                 table_name, partition_column, lower_bound, upper_bound,
                                 num_partitions, port=None, **connection_options):
        """Extract data with optimal partitioning for parallel processing"""
        print(f"\n=== Partitioned Extraction from {db_type.upper()} ===")

        # Build connection URL
        actual_port = port or self.db_configs[db_type]["port"]
        url = self.db_configs[db_type]["url_format"].format(
            server=server, port=actual_port, database=database
        )

        # Get optimized properties
        properties = self.get_optimized_connection_properties(
            db_type, username, password, **connection_options
        )

        # Read with partitioning
        df = self.spark.read \
            .jdbc(url=url,
                  table=table_name,
                  column=partition_column,
                  lowerBound=lower_bound,
                  upperBound=upper_bound,
                  numPartitions=num_partitions,
                  properties=properties)

        print(f"Partitioned read: {num_partitions} partitions on column '{partition_column}'")
        print(f"Bounds: {lower_bound} to {upper_bound}")
        print(f"Extracted {df.count():,} rows from {table_name}")

        # Analyze partition distribution
        partition_sizes = df.rdd.glom().map(len).collect()
        print(f"Partition sizes: min={min(partition_sizes)}, max={max(partition_sizes)}, avg={sum(partition_sizes)/len(partition_sizes):.0f}")

        return df

    def extract_with_custom_partitioning(self, db_type, server, database, username, password,
                                       table_name, partition_predicates, port=None, **connection_options):
        """Extract data using custom partition predicates for complex partitioning logic"""
        print(f"\n=== Custom Partitioned Extraction ===")

        actual_port = port or self.db_configs[db_type]["port"]
        url = self.db_configs[db_type]["url_format"].format(
            server=server, port=actual_port, database=database
        )

        properties = self.get_optimized_connection_properties(
            db_type, username, password, **connection_options
        )

        # Read with custom predicates
        df = self.spark.read \
            .jdbc(url=url,
                  table=table_name,
                  predicates=partition_predicates,
                  properties=properties)

        print(f"Custom partitioning with {len(partition_predicates)} predicates:")
        for i, predicate in enumerate(partition_predicates):
            print(f"  Partition {i+1}: {predicate}")

        return df

    def extract_with_query_partitioning(self, db_type, server, database, username, password,
                                      query, partition_column, lower_bound, upper_bound,
                                      num_partitions, port=None, **connection_options):
        """Extract data using SQL query with partitioning"""
        print(f"\n=== Query-based Partitioned Extraction ===")

        actual_port = port or self.db_configs[db_type]["port"]
        url = self.db_configs[db_type]["url_format"].format(
            server=server, port=actual_port, database=database
        )

        properties = self.get_optimized_connection_properties(
            db_type, username, password, **connection_options
        )

        # Wrap query in subquery for partitioning
        subquery = f"({query}) as partitioned_query"

        df = self.spark.read \
            .jdbc(url=url,
                  table=subquery,
                  column=partition_column,
                  lowerBound=lower_bound,
                  upperBound=upper_bound,
                  numPartitions=num_partitions,
                  properties=properties)

        print(f"Query partitioned on '{partition_column}' with {num_partitions} partitions")
        return df

    def extract_with_date_partitioning(self, db_type, server, database, username, password,
                                     table_name, date_column, start_date, end_date,
                                     partition_strategy="monthly", port=None, **connection_options):
        """Extract data with date-based partitioning strategies"""
        print(f"\n=== Date-based Partitioned Extraction ({partition_strategy}) ===")

        from datetime import datetime, timedelta
        import calendar

        # Generate date-based predicates
        predicates = []
        current_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

        if partition_strategy == "daily":
            while current_date <= end_datetime:
                next_date = current_date + timedelta(days=1)
                predicate = f"{date_column} >= '{current_date.strftime('%Y-%m-%d')}' AND {date_column} < '{next_date.strftime('%Y-%m-%d')}'"
                predicates.append(predicate)
                current_date = next_date

        elif partition_strategy == "weekly":
            while current_date <= end_datetime:
                next_date = current_date + timedelta(weeks=1)
                predicate = f"{date_column} >= '{current_date.strftime('%Y-%m-%d')}' AND {date_column} < '{next_date.strftime('%Y-%m-%d')}'"
                predicates.append(predicate)
                current_date = next_date

        elif partition_strategy == "monthly":
            while current_date <= end_datetime:
                year = current_date.year
                month = current_date.month
                last_day = calendar.monthrange(year, month)[1]
                next_date = datetime(year, month, last_day) + timedelta(days=1)
                predicate = f"{date_column} >= '{current_date.strftime('%Y-%m-%d')}' AND {date_column} < '{next_date.strftime('%Y-%m-%d')}'"
                predicates.append(predicate)
                current_date = next_date

        return self.extract_with_custom_partitioning(
            db_type, server, database, username, password,
            table_name, predicates, port, **connection_options
        )

    def extract_with_optimized_partitioning(self, db_type, server, database, username, password,
                                           table_name, partition_column, lower_bound, upper_bound,
                                           estimated_rows, port=None, **connection_options):
        """Extract data with automatic optimization based on estimated row count"""
        print(f"\n=== Optimized Partitioned Extraction ===")

        # Get optimization recommendations
        recommendations = self.optimize_connection_for_read_performance(
            db_type, table_name, estimated_rows
        )

        # Extract optimization parameters
        num_partitions = recommendations.pop("num_partitions")
        fetchsize = recommendations.pop("fetchsize")

        # Combine recommendations with user-provided options
        optimized_options = {**recommendations, **connection_options, "fetchsize": fetchsize}

        # Call the standard partitioned extraction with optimized settings
        return self.extract_with_partitioning(
            db_type=db_type,
            server=server,
            database=database,
            username=username,
            password=password,
            table_name=table_name,
            partition_column=partition_column,
            lower_bound=lower_bound,
            upper_bound=upper_bound,
            num_partitions=num_partitions,
            port=port,
            **optimized_options
        )

    def extract_with_optimized_custom_partitioning(self, db_type, server, database, username, password,
                                                 table_name, partition_predicates, estimated_rows,
                                                 port=None, **connection_options):
        """Extract data with custom partitioning and automatic fetch size optimization"""
        print(f"\n=== Optimized Custom Partitioned Extraction ===")

        # Get optimization recommendations (mainly for fetchsize)
        recommendations = self.optimize_connection_for_read_performance(
            db_type, table_name, estimated_rows
        )

        # Extract fetchsize and database-specific optimizations
        fetchsize = recommendations.pop("fetchsize")
        recommendations.pop("num_partitions")  # Not relevant for custom partitioning

        # Combine recommendations with user-provided options
        optimized_options = {**recommendations, **connection_options, "fetchsize": fetchsize}

        # Call the custom partitioned extraction with optimized settings
        return self.extract_with_custom_partitioning(
            db_type=db_type,
            server=server,
            database=database,
            username=username,
            password=password,
            table_name=table_name,
            partition_predicates=partition_predicates,
            port=port,
            **optimized_options
        )

    def optimize_connection_for_read_performance(self, db_type, table_name, estimated_rows):
        """Provide optimized connection settings based on data size"""
        print(f"\n=== Connection Optimization Recommendations ===")

        # Calculate optimal partitions based on data size
        target_partition_size = 100000  # Target rows per partition
        recommended_partitions = max(1, min(200, estimated_rows // target_partition_size))

        recommendations = {
            "num_partitions": recommended_partitions,
            "fetchsize": min(10000, max(1000, estimated_rows // recommended_partitions // 10))
        }

        # Database-specific recommendations
        if db_type == "sqlserver":
            recommendations.update({
                "packetSize": "32768" if estimated_rows > 1000000 else "8192",
                "responseBuffering": "adaptive" if estimated_rows > 500000 else "full",
                "selectMethod": "cursor" if estimated_rows > 100000 else "direct"
            })
        elif db_type == "mysql":
            recommendations.update({
                "useCursorFetch": "true" if estimated_rows > 100000 else "false",
                "defaultFetchSize": str(recommendations["fetchsize"])
            })
        elif db_type == "postgresql":
            recommendations.update({
                "defaultRowFetchSize": str(recommendations["fetchsize"]),
                "prepareThreshold": "3" if estimated_rows > 50000 else "0"
            })
        elif db_type == "oracle":
            recommendations.update({
                "oracle.jdbc.defaultRowPrefetch": str(min(50, recommendations["fetchsize"] // 100)),
                "oracle.jdbc.useFetchSizeWithLongColumn": "true"
            })

        print(f"Estimated rows: {estimated_rows:,}")
        print(f"Recommended partitions: {recommendations['num_partitions']}")
        print(f"Recommended fetch size: {recommendations['fetchsize']}")
        print("Database-specific optimizations:")
        for key, value in recommendations.items():
            if key not in ["num_partitions", "fetchsize"]:
                print(f"  {key}: {value}")

        return recommendations

    def write_to_database(self, df, db_type, server, database, username, password,
                         table_name, mode="append", port=None, batch_size=10000, **connection_options):
        """Write DataFrame to database with optimization"""
        print(f"\n=== Writing to {db_type.upper()} Database ===")

        actual_port = port or self.db_configs[db_type]["port"]
        url = self.db_configs[db_type]["url_format"].format(
            server=server, port=actual_port, database=database
        )

        # Add write-specific optimizations
        write_properties = self.get_optimized_connection_properties(
            db_type, username, password, **connection_options
        )

        # Add batch size for write optimization
        write_properties["batchsize"] = str(batch_size)
        write_properties["isolationLevel"] = "READ_UNCOMMITTED"  # For faster writes

        # Database-specific write optimizations
        if db_type == "sqlserver":
            write_properties.update({
                "bulkCopyOptions": "FIRE_TRIGGERS,CHECK_CONSTRAINTS",
                "bulkCopyBatchSize": str(batch_size),
                "bulkCopyTimeout": "0"
            })
        elif db_type == "mysql":
            write_properties.update({
                "rewriteBatchedStatements": "true",
                "useServerPrepStmts": "false"  # Better for bulk inserts
            })

        # Optimize DataFrame for writing
        row_count = df.count()
        optimal_partitions = max(1, min(20, row_count // 50000))
        optimized_df = df.repartition(optimal_partitions)

        # Write to database
        optimized_df.write \
            .jdbc(url=url, table=table_name, mode=mode, properties=write_properties)

        print(f"Successfully wrote {row_count:,} rows to {table_name}")
        print(f"Write mode: {mode}, Batch size: {batch_size}")

# Example usage and connection patterns
def demonstrate_jdbc_patterns():
    """Demonstrate various JDBC connection patterns"""
    connector = SparkJDBCConnector()

    # Example 1: SQL Server with partitioning
    print("=== SQL Server Partitioned Read Example ===")
    """
    df_sqlserver = connector.extract_with_partitioning(
        db_type="sqlserver",
        server="your-server.database.windows.net",
        database="your_database", 
        username="your_username",
        password="your_password",
        table_name="large_table",
        partition_column="id",
        lower_bound=1,
        upper_bound=1000000,
        num_partitions=10
    )
    """

    # Example 2: Custom date-based partitioning
    print("=== Date-based Partitioning Example ===")
    """
    df_date_partitioned = connector.extract_with_date_partitioning(
        db_type="postgresql",
        server="localhost",
        database="analytics_db",
        username="user",
        password="password",
        table_name="transactions",
        date_column="transaction_date",
        start_date="2024-01-01",
        end_date="2024-12-31",
        partition_strategy="monthly"
    )
    """

    # Example 3: Custom predicates for complex partitioning
    print("=== Custom Predicates Example ===")
    """
    custom_predicates = [
        "region = 'North' AND status = 'active'",
        "region = 'South' AND status = 'active'", 
        "region = 'East' AND status = 'active'",
        "region = 'West' AND status = 'active'",
        "status = 'inactive'"
    ]
    
    df_custom = connector.extract_with_custom_partitioning(
        db_type="mysql",
        server="localhost",
        database="sales_db",
        username="user", 
        password="password",
        table_name="customer_data",
        partition_predicates=custom_predicates
    )
    """

if __name__ == "__main__":
    demonstrate_jdbc_patterns()
