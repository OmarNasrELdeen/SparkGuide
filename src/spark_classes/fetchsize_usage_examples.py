"""
FetchSize Usage Examples for Spark JDBC Connections
This module demonstrates proper fetchSize usage and optimization strategies
"""

from spark_sql_connection import SparkJDBCConnector

class FetchSizeOptimizer:
    def __init__(self, app_name="FetchSizeOptimizer"):
        """Initialize FetchSize optimizer with JDBC connector"""
        self.connector = SparkJDBCConnector(app_name)

    def demonstrate_fetchsize_usage(self):
        """Demonstrate proper fetchSize usage patterns"""
        print("=" * 80)
        print("FETCHSIZE USAGE EXAMPLES")
        print("=" * 80)

        # Example 1: Manual fetchSize specification
        print("\n1. MANUAL FETCHSIZE SPECIFICATION")
        print("-" * 40)
        print("✓ FetchSize manually set to 5000 rows per fetch")
        print("✓ Will be applied as universal JDBC 'fetchsize' property")
        print("✓ Database-specific properties will also be set appropriately")

        # Example 2: Automatic optimization
        print("\n2. AUTOMATIC FETCHSIZE OPTIMIZATION")
        print("-" * 40)
        print("✓ FetchSize automatically calculated based on estimated_rows")
        print("✓ Optimal partition count also calculated")
        print("✓ Database-specific optimizations applied")

        # Example 3: Using optimization recommendations
        print("\n3. USING OPTIMIZATION RECOMMENDATIONS")
        print("-" * 40)
        print("✓ Get optimization recommendations separately")
        print("✓ Apply recommended fetchSize and other optimizations")
        print("✓ Full control over parameters while benefiting from optimization")

    def demonstrate_database_specific_fetchsize(self):
        """Show how fetchSize is handled for different databases"""
        print("\n" + "=" * 80)
        print("DATABASE-SPECIFIC FETCHSIZE HANDLING")
        print("=" * 80)

        databases = {
            "SQL Server": {
                "universal_property": "fetchsize=3000",
                "specific_optimizations": [
                    "packetSize: Optimized for large datasets",
                    "selectMethod: cursor for memory efficiency",
                    "responseBuffering: adaptive for large results"
                ]
            },
            "MySQL": {
                "universal_property": "fetchsize=3000",
                "specific_optimizations": [
                    "defaultFetchSize=3000 (MySQL driver specific)",
                    "useCursorFetch=true (for large datasets)",
                    "useServerPrepStmts=true (better performance)"
                ]
            },
            "PostgreSQL": {
                "universal_property": "fetchsize=3000",
                "specific_optimizations": [
                    "defaultRowFetchSize=3000 (PostgreSQL driver specific)",
                    "prepareThreshold: Optimized based on data size",
                    "preparedStatementCacheQueries: Enhanced caching"
                ]
            },
            "Oracle": {
                "universal_property": "fetchsize=3000",
                "specific_optimizations": [
                    "oracle.jdbc.defaultRowPrefetch=30 (calculated from fetchsize)",
                    "useFetchSizeWithLongColumn=true (handle LONG columns)",
                    "oracle.jdbc.ReadTimeout: Optimized for long queries"
                ]
            }
        }

        for i, (db_name, config) in enumerate(databases.items(), 1):
            print(f"\n{i}. {db_name.upper()}")
            print("-" * 20)
            print(f"Universal JDBC property: {config['universal_property']}")
            print("Additional optimizations:")
            for optimization in config['specific_optimizations']:
                print(f"  - {optimization}")

    def demonstrate_fetchsize_performance_impact(self):
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

    def demonstrate_best_practices(self):
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
                "practice": "Consider Database Type",
                "description": "Different databases have different optimal fetchSize ranges",
                "example": "PostgreSQL: 1000-10000, SQL Server: 1000-50000"
            },
            {
                "practice": "Balance with Partitioning",
                "description": "More partitions allow higher fetchSize per partition",
                "example": "100 partitions × 10000 fetchSize = good parallelism"
            },
            {
                "practice": "Test with Real Data",
                "description": "Optimal fetchSize depends on actual row size and network",
                "example": "Benchmark different fetchSize values with your data"
            }
        ]

        for i, practice in enumerate(best_practices, 1):
            print(f"\n{i}. {practice['practice'].upper()}")
            print("-" * 40)
            print(f"Description: {practice['description']}")
            print(f"Example: {practice['example']}")

    def calculate_optimal_fetchsize(self, estimated_rows, target_memory_per_partition_mb=100):
        """Calculate optimal fetchSize based on dataset characteristics"""
        print("\n" + "=" * 80)
        print("OPTIMAL FETCHSIZE CALCULATION")
        print("=" * 80)

        # Estimate row size (bytes) - can be customized
        estimated_row_size_bytes = 1024  # 1KB per row default

        # Calculate maximum rows per partition for target memory
        max_rows_per_partition = (target_memory_per_partition_mb * 1024 * 1024) // estimated_row_size_bytes

        # Calculate optimal partitions
        optimal_partitions = max(1, estimated_rows // max_rows_per_partition)
        optimal_partitions = min(optimal_partitions, 200)  # Cap at 200 partitions

        # Calculate rows per partition
        rows_per_partition = estimated_rows // optimal_partitions

        # Calculate optimal fetchSize (10-20% of rows per partition)
        optimal_fetchsize = max(1000, min(rows_per_partition // 10, 50000))

        calculation_details = {
            "estimated_rows": estimated_rows,
            "estimated_row_size_bytes": estimated_row_size_bytes,
            "target_memory_per_partition_mb": target_memory_per_partition_mb,
            "optimal_partitions": optimal_partitions,
            "rows_per_partition": rows_per_partition,
            "optimal_fetchsize": optimal_fetchsize,
            "estimated_total_memory_mb": (optimal_partitions * optimal_fetchsize * estimated_row_size_bytes) // (1024 * 1024)
        }

        print(f"Input Parameters:")
        print(f"  Estimated rows: {estimated_rows:,}")
        print(f"  Estimated row size: {estimated_row_size_bytes:,} bytes")
        print(f"  Target memory per partition: {target_memory_per_partition_mb} MB")
        print()
        print(f"Calculated Recommendations:")
        print(f"  Optimal partitions: {optimal_partitions}")
        print(f"  Rows per partition: {rows_per_partition:,}")
        print(f"  Optimal fetchSize: {optimal_fetchsize:,}")
        print(f"  Estimated total memory: {calculation_details['estimated_total_memory_mb']} MB")

        return calculation_details

    def benchmark_fetchsize_performance(self, test_scenarios):
        """Benchmark different fetchSize values (simulation)"""
        print("\n" + "=" * 80)
        print("FETCHSIZE PERFORMANCE BENCHMARKING")
        print("=" * 80)

        # Simulate performance results for different fetchSize values
        fetchsize_options = [1000, 5000, 10000, 25000, 50000]

        for scenario_name, scenario_config in test_scenarios.items():
            print(f"\n=== {scenario_name.upper()} ===")
            print(f"Dataset size: {scenario_config['rows']:,} rows")
            print(f"Partitions: {scenario_config['partitions']}")
            print()
            print(f"{'FetchSize':<10} {'Est. Time (s)':<15} {'Memory (MB)':<12} {'Recommendation'}")
            print("-" * 60)

            for fetchsize in fetchsize_options:
                # Simulate performance metrics (in real scenario, these would be actual measurements)
                est_time = max(10, 100 - (fetchsize / 1000))  # Simulate faster with higher fetchSize
                memory_mb = (fetchsize * scenario_config['partitions'] * 1024) // (1024 * 1024)

                recommendation = ""
                if fetchsize == 5000:
                    recommendation = "← Balanced"
                elif fetchsize == 10000 and scenario_config['rows'] > 1000000:
                    recommendation = "← Good for large data"
                elif fetchsize == 1000 and scenario_config['rows'] < 100000:
                    recommendation = "← Good for small data"

                print(f"{fetchsize:<10} {est_time:<15.1f} {memory_mb:<12} {recommendation}")

    def run_all_demonstrations(self):
        """Run all fetchSize demonstrations"""
        self.demonstrate_fetchsize_usage()
        self.demonstrate_database_specific_fetchsize()
        self.demonstrate_fetchsize_performance_impact()
        self.demonstrate_best_practices()

        # Example calculations
        self.calculate_optimal_fetchsize(1000000, 100)  # 1M rows, 100MB per partition
        self.calculate_optimal_fetchsize(10000000, 200)  # 10M rows, 200MB per partition

        # Example benchmarking scenarios
        test_scenarios = {
            "small_dataset": {"rows": 100000, "partitions": 2},
            "medium_dataset": {"rows": 1000000, "partitions": 10},
            "large_dataset": {"rows": 10000000, "partitions": 100}
        }
        self.benchmark_fetchsize_performance(test_scenarios)

# Example usage
if __name__ == "__main__":
    optimizer = FetchSizeOptimizer()
    optimizer.run_all_demonstrations()
