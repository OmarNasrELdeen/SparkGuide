"""
Enhanced Dataset Generator for Comprehensive ETL Testing
Generates diverse datasets for staging-to-curated database workflows
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random
import string
import json

class DatasetGenerator:
    def __init__(self, spark_session=None):
        """Initialize dataset generator with Spark session"""
        if spark_session:
            self.spark = spark_session
        else:
            self.spark = SparkSession.builder \
                .appName("DatasetGenerator") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()

    def generate_sales_data(self, num_records, skew_factor=0.2):
        """Generate sales data with configurable volume and skew"""
        print(f"Generating {num_records:,} sales records...")

        categories = ["Electronics", "Clothing", "Books", "Home", "Sports"]
        regions = ["North", "South", "East", "West", "Central"]
        products = [f"Product_{i}" for i in range(100)]
        statuses = ["completed", "pending", "cancelled"]

        # Create skewed data - some categories appear more frequently
        if skew_factor > 0:
            skewed_categories = ["Electronics"] * int(num_records * skew_factor)
            normal_categories = [random.choice(categories) for _ in range(num_records - len(skewed_categories))]
            all_categories = skewed_categories + normal_categories
            random.shuffle(all_categories)
        else:
            all_categories = [random.choice(categories) for _ in range(num_records)]

        base_date = datetime(2023, 1, 1)
        data = []

        for i in range(num_records):
            date = base_date + timedelta(days=random.randint(0, 730))  # 2 years range
            data.append((
                i,
                all_categories[i],
                random.choice(regions),
                random.choice(products),
                random.randint(10, 10000),  # sales amount
                date.strftime("%Y-%m-%d"),
                random.choice(statuses),
                f"customer_{random.randint(1, min(1000, num_records//10))}",  # customer churn
                random.randint(1, 100)  # quantity
            ))

        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("category", StringType(), True),
            StructField("region", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("sales", IntegerType(), True),
            StructField("date", StringType(), True),
            StructField("status", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("quantity", IntegerType(), True)
        ])

        df = self.spark.createDataFrame(data, schema)
        df = df.withColumn("date", to_date(col("date")))

        print(f"Generated dataset with {df.count():,} records")
        return df

    def generate_financial_data(self, num_records):
        """Generate financial transaction data"""
        print(f"Generating {num_records:,} financial records...")

        account_types = ["checking", "savings", "credit", "investment"]
        transaction_types = ["deposit", "withdrawal", "transfer", "payment"]
        currencies = ["USD", "EUR", "GBP", "JPY"]

        data = []
        base_date = datetime(2023, 1, 1)

        for i in range(num_records):
            data.append((
                i,
                f"account_{random.randint(1, min(10000, num_records//5))}",
                random.choice(account_types),
                random.choice(transaction_types),
                round(random.uniform(1.0, 50000.0), 2),
                random.choice(currencies),
                (base_date + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d %H:%M:%S"),
                f"branch_{random.randint(1, 50)}"
            ))

        schema = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("account_id", StringType(), True),
            StructField("account_type", StringType(), True),
            StructField("transaction_type", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("branch_id", StringType(), True)
        ])

        df = self.spark.createDataFrame(data, schema)
        df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

        return df

    def generate_time_series_data(self, num_records, frequency="daily"):
        """Generate time series data for testing temporal operations"""
        print(f"Generating {num_records:,} time series records with {frequency} frequency...")

        sensors = [f"sensor_{i}" for i in range(20)]
        locations = ["warehouse_A", "warehouse_B", "warehouse_C", "warehouse_D"]

        data = []
        base_date = datetime(2023, 1, 1)

        # Calculate time increment based on frequency
        if frequency == "hourly":
            increment = timedelta(hours=1)
        elif frequency == "daily":
            increment = timedelta(days=1)
        elif frequency == "weekly":
            increment = timedelta(weeks=1)
        else:
            increment = timedelta(days=1)

        for i in range(num_records):
            timestamp = base_date + (increment * i)
            data.append((
                i,
                random.choice(sensors),
                random.choice(locations),
                round(random.uniform(-10.0, 50.0), 2),  # temperature
                round(random.uniform(30.0, 90.0), 2),   # humidity
                round(random.uniform(0.0, 100.0), 2),   # pressure
                timestamp.strftime("%Y-%m-%d %H:%M:%S")
            ))

        schema = StructType([
            StructField("reading_id", IntegerType(), True),
            StructField("sensor_id", StringType(), True),
            StructField("location", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("pressure", DoubleType(), True),
            StructField("timestamp", StringType(), True)
        ])

        df = self.spark.createDataFrame(data, schema)
        df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

        return df

    def generate_nested_data(self, num_records):
        """Generate nested/complex data structures for testing"""
        print(f"Generating {num_records:,} nested structure records...")

        data = []
        for i in range(num_records):
            # Nested structure: customer with orders and items
            customer_data = {
                "customer_id": f"cust_{i}",
                "personal_info": {
                    "name": f"Customer_{i}",
                    "age": random.randint(18, 80),
                    "email": f"customer_{i}@email.com"
                },
                "orders": [
                    {
                        "order_id": f"order_{i}_{j}",
                        "date": (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
                        "items": [
                            {
                                "item_id": f"item_{k}",
                                "quantity": random.randint(1, 10),
                                "price": round(random.uniform(10.0, 500.0), 2)
                            } for k in range(random.randint(1, 5))
                        ]
                    } for j in range(random.randint(1, 3))
                ]
            }
            data.append((json.dumps(customer_data),))

        schema = StructType([
            StructField("customer_json", StringType(), True)
        ])

        df = self.spark.createDataFrame(data, schema)

        # Parse JSON into proper nested structure
        df = df.withColumn("customer_data", from_json(col("customer_json"),
            StructType([
                StructField("customer_id", StringType(), True),
                StructField("personal_info", StructType([
                    StructField("name", StringType(), True),
                    StructField("age", IntegerType(), True),
                    StructField("email", StringType(), True)
                ]), True),
                StructField("orders", ArrayType(StructType([
                    StructField("order_id", StringType(), True),
                    StructField("date", StringType(), True),
                    StructField("items", ArrayType(StructType([
                        StructField("item_id", StringType(), True),
                        StructField("quantity", IntegerType(), True),
                        StructField("price", DoubleType(), True)
                    ])), True)
                ])), True)
            ])
        )).drop("customer_json")

        return df

    def generate_customer_master_data(self, num_customers):
        """Generate customer master data for staging-to-curated workflow"""
        print(f"Generating {num_customers:,} customer master records...")

        customer_data = []
        for i in range(num_customers):
            customer_data.append((
                f"CUST_{i:08d}",
                f"Customer_{i}",
                f"customer{i}@email.com",
                random.choice(["Individual", "Corporate", "Government"]),
                random.choice(["Active", "Inactive", "Suspended"]),
                random.choice(["North", "South", "East", "West", "Central"]),
                f"{random.randint(10000, 99999)}",
                random.choice(["USA", "Canada", "Mexico", "UK", "Germany"]),
                (datetime.now() - timedelta(days=random.randint(1, 3650))).strftime("%Y-%m-%d"),
                random.choice([None, "VIP", "Premium", "Standard"]),
                random.uniform(0, 100000)
            ))

        schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("customer_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("customer_type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("region", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("tier", StringType(), True),
            StructField("credit_limit", DoubleType(), True)
        ])

        df = self.spark.createDataFrame(customer_data, schema)
        return df.withColumn("created_date", to_date(col("created_date")))

    def generate_product_catalog_data(self, num_products):
        """Generate product catalog data for staging-to-curated workflow"""
        print(f"Generating {num_products:,} product catalog records...")

        categories = ["Electronics", "Clothing", "Books", "Home", "Sports", "Beauty", "Automotive", "Toys"]
        brands = ["Brand_A", "Brand_B", "Brand_C", "Brand_D", "Brand_E"]

        product_data = []
        for i in range(num_products):
            category = random.choice(categories)
            brand = random.choice(brands)

            product_data.append((
                f"PROD_{i:08d}",
                f"{brand}_{category}_Product_{i}",
                category,
                brand,
                round(random.uniform(10.0, 2000.0), 2),
                round(random.uniform(5.0, 1500.0), 2),
                random.choice(["Active", "Discontinued", "Coming Soon"]),
                random.randint(0, 10000),
                (datetime.now() - timedelta(days=random.randint(1, 1095))).strftime("%Y-%m-%d"),
                random.choice([None, "On Sale", "New Arrival", "Best Seller"]),
                f"Description for {category} product {i}"
            ))

        schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("list_price", DoubleType(), True),
            StructField("cost_price", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("inventory_qty", IntegerType(), True),
            StructField("launch_date", StringType(), True),
            StructField("promotion_tag", StringType(), True),
            StructField("description", StringType(), True)
        ])

        df = self.spark.createDataFrame(product_data, schema)
        return df.withColumn("launch_date", to_date(col("launch_date")))

    def generate_transaction_data_with_quality_issues(self, num_transactions, customer_df, product_df):
        """Generate transaction data with data quality issues for testing"""
        print(f"Generating {num_transactions:,} transaction records with quality issues...")

        # Get sample customer and product IDs
        customer_ids = [row.customer_id for row in customer_df.select("customer_id").sample(0.1).collect()]
        product_ids = [row.product_id for row in product_df.select("product_id").sample(0.1).collect()]

        transaction_data = []
        for i in range(num_transactions):
            # Introduce data quality issues
            has_quality_issue = random.random() < 0.15  # 15% chance of quality issues

            if has_quality_issue:
                issue_type = random.choice([
                    "null_customer", "invalid_product", "negative_amount",
                    "future_date", "missing_quantity", "duplicate"
                ])

                if issue_type == "null_customer":
                    customer_id = None
                elif issue_type == "invalid_product":
                    customer_id = random.choice(customer_ids)
                    product_id = "INVALID_PROD"
                elif issue_type == "negative_amount":
                    customer_id = random.choice(customer_ids)
                    product_id = random.choice(product_ids)
                    amount = -random.uniform(10, 1000)
                elif issue_type == "future_date":
                    customer_id = random.choice(customer_ids)
                    product_id = random.choice(product_ids)
                    transaction_date = (datetime.now() + timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d")
                elif issue_type == "missing_quantity":
                    customer_id = random.choice(customer_ids)
                    product_id = random.choice(product_ids)
                    quantity = None
                else:  # duplicate
                    customer_id = random.choice(customer_ids)
                    product_id = random.choice(product_ids)
            else:
                customer_id = random.choice(customer_ids)
                product_id = random.choice(product_ids)

            # Set default values if not set above
            if 'amount' not in locals():
                amount = round(random.uniform(10.0, 5000.0), 2)
            if 'transaction_date' not in locals():
                transaction_date = (datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d")
            if 'quantity' not in locals():
                quantity = random.randint(1, 10)

            transaction_data.append((
                f"TXN_{i:010d}",
                customer_id,
                product_id,
                amount,
                quantity,
                transaction_date,
                random.choice(["Online", "Store", "Mobile", "Phone"]),
                random.choice(["Completed", "Pending", "Cancelled", "Refunded"]),
                f"Store_{random.randint(1, 100)}",
                round(amount * 0.08, 2) if amount and amount > 0 else None  # Tax
            ))

            # Reset variables for next iteration
            if 'amount' in locals():
                del amount
            if 'transaction_date' in locals():
                del transaction_date
            if 'quantity' in locals():
                del quantity

        schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("transaction_date", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("status", StringType(), True),
            StructField("store_id", StringType(), True),
            StructField("tax_amount", DoubleType(), True)
        ])

        df = self.spark.createDataFrame(transaction_data, schema)
        return df.withColumn("transaction_date", to_date(col("transaction_date")))

    def generate_staging_datasets_for_sql_server(self, output_config):
        """Generate complete staging datasets for SQL Server testing"""
        print("\n" + "="*80)
        print("GENERATING STAGING DATASETS FOR SQL SERVER")
        print("="*80)

        datasets = {}

        # Generate master data
        customer_df = self.generate_customer_master_data(output_config.get('customers', 10000))
        product_df = self.generate_product_catalog_data(output_config.get('products', 5000))

        # Generate transaction data with quality issues
        transaction_df = self.generate_transaction_data_with_quality_issues(
            output_config.get('transactions', 100000),
            customer_df,
            product_df
        )

        # Generate additional dimension data
        store_df = self.generate_store_dimension_data(output_config.get('stores', 200))

        datasets['staging_customers'] = customer_df
        datasets['staging_products'] = product_df
        datasets['staging_transactions'] = transaction_df
        datasets['staging_stores'] = store_df

        # Generate fact table data
        datasets['staging_sales_fact'] = self.generate_sales_fact_data(
            transaction_df, customer_df, product_df, store_df
        )

        return datasets

    def generate_store_dimension_data(self, num_stores):
        """Generate store dimension data"""
        print(f"Generating {num_stores:,} store records...")

        store_data = []
        for i in range(num_stores):
            store_data.append((
                f"Store_{i:03d}",
                f"Store Name {i}",
                random.choice(["Mall", "Standalone", "Outlet", "Online"]),
                random.choice(["North", "South", "East", "West", "Central"]),
                f"City_{random.randint(1, 50)}",
                random.choice(["Small", "Medium", "Large", "XLarge"]),
                (datetime.now() - timedelta(days=random.randint(30, 3650))).strftime("%Y-%m-%d"),
                random.choice(["Active", "Inactive", "Renovation"])
            ))

        schema = StructType([
            StructField("store_id", StringType(), False),
            StructField("store_name", StringType(), True),
            StructField("store_type", StringType(), True),
            StructField("region", StringType(), True),
            StructField("city", StringType(), True),
            StructField("size_category", StringType(), True),
            StructField("opening_date", StringType(), True),
            StructField("status", StringType(), True)
        ])

        df = self.spark.createDataFrame(store_data, schema)
        return df.withColumn("opening_date", to_date(col("opening_date")))

    def generate_sales_fact_data(self, transaction_df, customer_df, product_df, store_df):
        """Generate sales fact data by joining staging tables"""
        print("Generating sales fact data...")

        # Join transactions with dimensions
        sales_fact = transaction_df \
            .join(customer_df.select("customer_id", "region", "customer_type"), "customer_id", "left") \
            .join(product_df.select("product_id", "category", "brand", "list_price"), "product_id", "left") \
            .join(store_df.select("store_id", "store_type", "region").withColumnRenamed("region", "store_region"), "store_id", "left") \
            .withColumn("profit_margin",
                       when(col("list_price").isNotNull() & col("amount").isNotNull(),
                            col("amount") - (col("list_price") * col("quantity") * 0.7))
                       .otherwise(0)) \
            .withColumn("year", year(col("transaction_date"))) \
            .withColumn("month", month(col("transaction_date"))) \
            .withColumn("quarter", quarter(col("transaction_date")))

        return sales_fact

    def get_dataset_configs(self):
        """Return predefined dataset configurations for different volumes"""
        return {
            "small": {
                "records": 1000,
                "skew_factor": 0.1,
                "description": "Small dataset for quick testing"
            },
            "medium": {
                "records": 50000,
                "skew_factor": 0.2,
                "description": "Medium dataset for performance testing"
            },
            "large": {
                "records": 500000,
                "skew_factor": 0.3,
                "description": "Large dataset for stress testing"
            },
            "xlarge": {
                "records": 2000000,
                "skew_factor": 0.4,
                "description": "Extra large dataset for scalability testing"
            }
        }

    def get_staging_dataset_configs(self):
        """Return predefined staging dataset configurations"""
        return {
            "development": {
                "customers": 1000,
                "products": 500,
                "stores": 20,
                "transactions": 10000,
                "description": "Small dataset for development testing"
            },
            "staging": {
                "customers": 10000,
                "products": 2000,
                "stores": 100,
                "transactions": 100000,
                "description": "Medium dataset for staging environment"
            },
            "production_like": {
                "customers": 100000,
                "products": 10000,
                "stores": 500,
                "transactions": 1000000,
                "description": "Large dataset simulating production volumes"
            },
            "stress_test": {
                "customers": 500000,
                "products": 50000,
                "stores": 1000,
                "transactions": 5000000,
                "description": "Extra large dataset for stress testing"
            }
        }

    def generate_all_staging_datasets(self, environment="staging"):
        """Generate all staging datasets for specified environment"""
        config = self.get_staging_dataset_configs()[environment]
        return self.generate_staging_datasets_for_sql_server(config)
