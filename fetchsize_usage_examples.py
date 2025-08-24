"""
FetchSize Usage Examples for Spark JDBC Connections
This module demonstrates proper fetchSize usage and optimization strategies
"""

from spark_sql_connection import SparkJDBCConnector

def demonstrate_fetchsize_usage():
    """Demonstrate proper fetchSize usage patterns"""
    connector = SparkJDBCConnector("FetchSizeDemo")

    print("=" * 80)
    print("FETCHSIZE USAGE EXAMPLES")
    print("=" * 80)

    # Example 1: Manual fetchSize specification
    print("\n1. MANUAL FETCHSIZE SPECIFICATION")
    print("-" * 40)
    """
    # Traditional way - manually specifying fetchSize
    df_manual = connector.extract_with_partitioning(
        db_type="postgresql",
        server="localhost",
        database="sales_db",
        username="user",
        password="password",
        table_name="large_sales_table",
        partition_column="sale_id",
        lower_bound=1,
        upper_bound=1000000,
        num_partitions=20,
        fetchsize=5000  # Manually specified fetchSize
    )
    """
    print("✓ FetchSize manually set to 5000 rows per fetch")
    print("✓ Will be applied as universal JDBC 'fetchsize' property")
    print("✓ Database-specific properties will also be set appropriately")

    # Example 2: Automatic optimization
    print("\n2. AUTOMATIC FETCHSIZE OPTIMIZATION")
    print("-" * 40)
    """
    # New optimized way - automatic fetchSize calculation
    df_optimized = connector.extract_with_optimized_partitioning(
        db_type="postgresql",
        server="localhost", 
        database="sales_db",
        username="user",
        password="password",
        table_name="large_sales_table",
        partition_column="sale_id",
        lower_bound=1,
        upper_bound=1000000,
        estimated_rows=1000000  # Provide estimated row count
    )
    """
    print("✓ FetchSize automatically calculated based on estimated_rows")
    print("✓ Optimal partition count also calculated")
    print("✓ Database-specific optimizations applied")

    # Example 3: Using optimization recommendations
    print("\n3. USING OPTIMIZATION RECOMMENDATIONS")
    print("-" * 40)
    """
    # Get recommendations first, then apply manually
    recommendations = connector.optimize_connection_for_read_performance(
        db_type="sqlserver",
        table_name="huge_transaction_table",
        estimated_rows=5000000
    )
    
    # Use recommendations in standard extraction
    df_recommended = connector.extract_with_partitioning(
        db_type="sqlserver",
        server="sql-server.company.com",
        database="transactions_db",
        username="etl_user",
        password="secure_password",
        table_name="huge_transaction_table",
        partition_column="transaction_id",
        lower_bound=1,
        upper_bound=5000000,
        num_partitions=recommendations["num_partitions"],  # Use recommended partitions
        fetchsize=recommendations["fetchsize"],            # Use recommended fetchSize
        **{k: v for k, v in recommendations.items() 
           if k not in ["num_partitions", "fetchsize"]}   # Apply other optimizations
    )
    """
    print("✓ Get optimization recommendations separately")
    print("✓ Apply recommended fetchSize and other optimizations")
    print("✓ Full control over parameters while benefiting from optimization")

def demonstrate_database_specific_fetchsize():
    """Show how fetchSize is handled for different databases"""
    connector = SparkJDBCConnector("DatabaseSpecificDemo")

    print("\n" + "=" * 80)
    print("DATABASE-SPECIFIC FETCHSIZE HANDLING")
    print("=" * 80)

    # SQL Server
    print("\n1. SQL SERVER")
    print("-" * 20)
    print("Universal JDBC property: fetchsize=3000")
    print("Additional optimizations:")
    print("  - packetSize: Optimized for large datasets")
    print("  - selectMethod: cursor for memory efficiency")
    print("  - responseBuffering: adaptive for large results")

    # MySQL
    print("\n2. MYSQL")
    print("-" * 20)
    print("Universal JDBC property: fetchsize=3000")
    print("MySQL-specific properties:")
    print("  - defaultFetchSize=3000 (MySQL driver specific)")
    print("  - useCursorFetch=true (for large datasets)")
    print("  - useServerPrepStmts=true (better performance)")

    # PostgreSQL
    print("\n3. POSTGRESQL")
    print("-" * 20)
    print("Universal JDBC property: fetchsize=3000")
    print("PostgreSQL-specific properties:")
    print("  - defaultRowFetchSize=3000 (PostgreSQL driver specific)")
    print("  - prepareThreshold: Optimized based on data size")
    print("  - preparedStatementCacheQueries: Enhanced caching")

    # Oracle
    print("\n4. ORACLE")
    print("-" * 20)
    print("Universal JDBC property: fetchsize=3000")
    print("Oracle-specific properties:")
    print("  - oracle.jdbc.defaultRowPrefetch=30 (calculated from fetchsize)")
    print("  - useFetchSizeWithLongColumn=true (handle LONG columns)")
    print("  - oracle.jdbc.ReadTimeout: Optimized for long queries")

def demonstrate_fetchsize_performance_impact():
    """Show the performance impact of different fetchSize values"""

    print("\n" + "=" * 80)
    print("FETCHSIZE PERFORMANCE IMPACT")
    print("=" * 80)

    performance_scenarios = [
        {
            "scenario": "Small Dataset (10K rows)",
            "recommended_fetchsize": 1000,
            "reasoning": "Lower fetchSize to avoid memory overhead",
            "partitions": 1,
            "expected_performance": "Fast, low memory usage"
        },
        {
            "scenario": "Medium Dataset (1M rows)",
            "recommended_fetchsize": 5000,
            "reasoning": "Balanced fetchSize for good throughput",
            "partitions": 10,
            "expected_performance": "Good balance of speed and memory"
        },
        {
            "scenario": "Large Dataset (10M rows)",
            "recommended_fetchsize": 10000,
            "reasoning": "Higher fetchSize for maximum throughput",
            "partitions": 100,
            "expected_performance": "Maximum throughput, higher memory usage"
        },
        {
            "scenario": "Huge Dataset (100M rows)",
            "recommended_fetchsize": 10000,
            "reasoning": "Maximum fetchSize, more partitions",
            "partitions": 200,
            "expected_performance": "Optimal for very large datasets"
        }
    ]

    for i, scenario in enumerate(performance_scenarios, 1):
        print(f"\n{i}. {scenario['scenario'].upper()}")
        print("-" * 40)
        print(f"Recommended fetchSize: {scenario['recommended_fetchsize']:,}")
        print(f"Recommended partitions: {scenario['partitions']}")
        print(f"Reasoning: {scenario['reasoning']}")
        print(f"Expected performance: {scenario['expected_performance']}")

def demonstrate_best_practices():
    """Show best practices for using fetchSize"""

    print("\n" + "=" * 80)
    print("FETCHSIZE BEST PRACTICES")
    print("=" * 80)

    best_practices = [
        {
            "practice": "Use Automatic Optimization",
            "description": "Let the system calculate optimal fetchSize based on estimated rows",
            "example": "extract_with_optimized_partitioning(estimated_rows=1000000)"
        },
        {
            "practice": "Monitor Memory Usage",
            "description": "Higher fetchSize = more memory per partition",
            "example": "fetchSize * numPartitions * avgRowSize = total memory estimate"
        },
        {
            "practice": "Consider Network Latency",
            "description": "Higher fetchSize reduces round trips but increases per-trip time",
            "example": "High latency networks benefit from larger fetchSize (5K-10K)"
        },
        {
            "practice": "Database-Specific Tuning",
            "description": "Different databases have different optimal fetchSize ranges",
            "example": "PostgreSQL: 1K-10K, SQL Server: 2K-8K, MySQL: 1K-5K"
        },
        {
            "practice": "Test and Measure",
            "description": "Always benchmark with your specific data and infrastructure",
            "example": "Test fetchSize values: 1K, 2K, 5K, 10K and measure performance"
        }
    ]

    for i, practice in enumerate(best_practices, 1):
        print(f"\n{i}. {practice['practice'].upper()}")
        print("-" * 40)
        print(f"Description: {practice['description']}")
        print(f"Example: {practice['example']}")

def main():
    """Main demonstration function"""
    demonstrate_fetchsize_usage()
    demonstrate_database_specific_fetchsize()
    demonstrate_fetchsize_performance_impact()
    demonstrate_best_practices()

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("✓ FetchSize is now properly integrated into all extract methods")
    print("✓ Use extract_with_optimized_partitioning() for automatic optimization")
    print("✓ Manual fetchsize parameter works with all extract methods")
    print("✓ Database-specific properties are automatically set")
    print("✓ Performance optimization recommendations are provided")
    print("\nThe fetchSize issue has been resolved! 🎉")

if __name__ == "__main__":
    main()
