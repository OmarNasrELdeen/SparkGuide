# Spark Advanced Transformations Documentation

## Overview
The `spark_advanced_transformations.py` module demonstrates advanced transformation techniques for complex ETL scenarios, including complex data types, string processing, date/time operations, window functions, and data quality transformations.

## Class: SparkAdvancedTransformations

### Initialization

```python
transformer = SparkAdvancedTransformations(app_name="SparkAdvancedETL")
```

#### Spark Configuration Options

| Configuration | Value | Purpose | Performance Impact |
|---------------|-------|---------|-------------------|
| `spark.sql.adaptive.enabled` | `"true"` | Enable Adaptive Query Execution | Better transformation optimization |
| `spark.sql.adaptive.coalescePartitions.enabled` | `"true"` | Auto-merge small partitions | Fewer small files after transformations |
| `spark.sql.adaptive.skewJoin.enabled` | `"true"` | Handle skewed data in joins | Better performance with skewed transformations |
| `spark.sql.execution.arrow.pyspark.enabled` | `"true"` | Enable Arrow for pandas conversions | 10-100x faster pandas operations |
| `spark.serializer` | `"KryoSerializer"` | Fast serialization | Better performance |

## Method: complex_data_type_operations()

### Purpose
Handles complex data types including arrays, maps, and structs for semi-structured and nested data processing.

### Parameters
- `df`: DataFrame containing complex data types

### Complex Data Type Operations

#### 1. Array Operations
```python
# Creating and manipulating arrays
array_df = df \
    .withColumn("tags", array(lit("electronics"), lit("mobile"), lit("smartphone"))) \
    .withColumn("tag_count", size(col("tags"))) \
    .withColumn("first_tag", col("tags")[0]) \
    .withColumn("last_tag", col("tags")[size(col("tags")) - 1]) \
    .withColumn("tags_string", concat_ws(",", col("tags")))

# Array functions
array_enhanced_df = array_df \
    .withColumn("has_mobile_tag", array_contains(col("tags"), "mobile")) \
    .withColumn("tags_sorted", sort_array(col("tags"))) \
    .withColumn("tags_reversed", reverse(col("tags"))) \
    .withColumn("unique_tags", array_distinct(col("tags")))
```

#### 2. Exploding Arrays (Array to Rows)
```python
# Convert array elements to separate rows
exploded_df = array_df \
    .select("id", "name", explode(col("tags")).alias("tag"))

# With position information
exploded_with_pos = array_df \
    .select("id", "name", posexplode(col("tags")).alias("tag_position", "tag"))

# Example transformation:
# Input:  id=1, tags=["mobile", "phone"]
# Output: id=1, tag="mobile"
#         id=1, tag="phone"
```

#### 3. Map Operations
```python
# Creating and working with maps
map_df = df \
    .withColumn("attributes", 
               create_map(
                   lit("category"), lit("electronics"),
                   lit("brand"), lit("apple"),
                   lit("model"), lit("iphone")
               )) \
    .withColumn("category", col("attributes")["category"]) \
    .withColumn("brand", col("attributes")["brand"]) \
    .withColumn("map_keys", map_keys(col("attributes"))) \
    .withColumn("map_values", map_values(col("attributes"))) \
    .withColumn("map_size", size(col("attributes")))
```

#### 4. Struct Operations
```python
# Creating and accessing structs
struct_df = df \
    .withColumn("customer_info", 
               struct(
                   col("name").alias("full_name"),
                   col("id").alias("customer_id"),
                   col("email").alias("email_address")
               )) \
    .withColumn("customer_name", col("customer_info.full_name")) \
    .withColumn("customer_email", col("customer_info.email_address"))

# Nested struct access
nested_struct_df = struct_df \
    .withColumn("contact_info", 
               struct(
                   col("customer_info").alias("customer"),
                   col("phone").alias("phone_number")
               )) \
    .withColumn("nested_name", col("contact_info.customer.full_name"))
```

**Example Usage:**
```python
# Sample e-commerce data with complex types
ecommerce_data = [
    (1, "John Doe", "john@email.com", "Electronics"),
    (2, "Jane Smith", "jane@email.com", "Clothing"),
    (3, "Bob Johnson", "bob@email.com", "Books")
]

df = spark.createDataFrame(ecommerce_data, ["id", "name", "email", "category"])

# Apply complex data type operations
array_df, exploded_df, map_df, struct_df = transformer.complex_data_type_operations(df)

# Results show:
# - Arrays: Product tags and categories
# - Maps: Product attributes and metadata
# - Structs: Customer information grouping
# - Exploded: Individual tag analysis
```

### Complex Data Type Use Cases

| Data Type | Use Case | Example | Performance |
|-----------|----------|---------|-------------|
| **Arrays** | Product categories, tags | `["electronics", "mobile"]` | Good |
| **Maps** | Configuration data | `{"color": "red", "size": "large"}` | Medium |
| **Structs** | Nested records | `{name: "John", address: {...}}` | Good |
| **Exploded Arrays** | Tag analysis | Array → multiple rows | Slower |

## Method: advanced_string_transformations()

### Purpose
Advanced string processing and text transformations for data cleaning and standardization.

### String Transformation Categories

#### 1. Data Cleaning and Normalization
```python
# Comprehensive string cleaning
cleaned_df = df \
    .withColumn("name_cleaned", 
               trim(                                    # Remove leading/trailing spaces
                   lower(                              # Convert to lowercase
                       regexp_replace(                 # Remove special characters
                           col("name"), 
                           "[^a-zA-Z0-9\\s]", ""
                       )
                   )
               )) \
    .withColumn("name_standardized", 
               regexp_replace(col("name_cleaned"), "\\s+", " "))  # Replace multiple spaces

# Phone number cleaning
phone_cleaned_df = df \
    .withColumn("phone_digits_only", regexp_replace(col("phone"), "[^0-9]", "")) \
    .withColumn("phone_formatted", 
               regexp_replace(col("phone_digits_only"), 
                            "(\\d{3})(\\d{3})(\\d{4})", 
                            "($1) $2-$3"))
```

#### 2. Text Extraction with Regular Expressions
```python
# Extract components from strings
extracted_df = df \
    .withColumn("email_domain", regexp_extract(col("email"), "@(.+)", 1)) \
    .withColumn("phone_area_code", regexp_extract(col("phone"), "\\((\\d{3})\\)", 1)) \
    .withColumn("name_parts", split(col("name"), " ")) \
    .withColumn("first_name", col("name_parts")[0]) \
    .withColumn("last_name", col("name_parts")[size(col("name_parts")) - 1])

# URL parsing
url_parsed_df = df \
    .withColumn("protocol", regexp_extract(col("url"), "^(https?)://", 1)) \
    .withColumn("domain", regexp_extract(col("url"), "://([^/]+)", 1)) \
    .withColumn("path", regexp_extract(col("url"), "://[^/]+(/.*)", 1))
```

#### 3. Advanced String Functions
```python
# String analysis and manipulation
string_analysis_df = df \
    .withColumn("text_length", length(col("description"))) \
    .withColumn("word_count", size(split(col("description"), "\\s+"))) \
    .withColumn("char_count_no_spaces", length(regexp_replace(col("description"), "\\s", ""))) \
    .withColumn("text_hash", hash(col("description"))) \
    .withColumn("text_md5", md5(col("description"))) \
    .withColumn("text_base64", base64(col("description").cast("binary"))) \
    .withColumn("soundex_code", soundex(col("name")))

# String similarity and matching
similarity_df = df \
    .withColumn("levenshtein_dist", levenshtein(col("name1"), col("name2"))) \
    .withColumn("names_similar", levenshtein(col("name1"), col("name2")) < 3)
```

#### 4. JSON Processing
```python
# Convert to JSON and parse JSON
json_df = df \
    .withColumn("record_json", 
               to_json(struct(col("id"), col("name"), col("amount")))) \
    .withColumn("metadata_json", 
               to_json(map_from_arrays(
                   array(lit("created_date"), lit("updated_date")),
                   array(col("created"), col("updated"))
               )))

# Parse JSON strings
parsed_json_df = df \
    .withColumn("parsed_data", 
               from_json(col("json_string"), 
                        StructType([
                            StructField("customer_id", StringType()),
                            StructField("order_date", StringType()),
                            StructField("items", ArrayType(StringType()))
                        ]))) \
    .withColumn("customer_id", col("parsed_data.customer_id")) \
    .withColumn("order_items", col("parsed_data.items"))
```

**Example Usage:**
```python
# Sample messy customer data
messy_data = [
    (1, "  John   Doe  ", "john.doe@GMAIL.com", "(555) 123-4567"),
    (2, "jane_smith!", "Jane.Smith@company.COM", "555.987.6543"),
    (3, "bob-johnson", "bob@domain.org", "5551234567")
]

messy_df = spark.createDataFrame(messy_data, ["id", "name", "email", "phone"])

# Apply string transformations
cleaned_df, extracted_df, string_ops_df, json_df = transformer.advanced_string_transformations(messy_df)

# Results show:
# - Cleaned names: "john doe", "jane smith", "bob johnson"
# - Extracted domains: "gmail.com", "company.com", "domain.org"
# - Formatted phones: "(555) 123-4567", "(555) 987-6543", "(555) 123-4567"
```

### String Processing Performance Tips

| Operation | Performance | Use Case | Alternative |
|-----------|-------------|----------|-------------|
| **Simple Functions** | Fast | Basic cleaning | Built-in functions |
| **Regex Operations** | Medium | Pattern matching | Simplify patterns |
| **Complex Parsing** | Slow | Structured extraction | Pre-process if possible |
| **JSON Processing** | Medium | Semi-structured data | Use appropriate schema |

## Method: advanced_date_time_operations()

### Purpose
Advanced date and time transformations for temporal data processing and business logic.

### Date/Time Transformation Categories

#### 1. Current Date/Time Operations
```python
# Current timestamp operations
datetime_df = df \
    .withColumn("current_timestamp", current_timestamp()) \
    .withColumn("current_date", current_date()) \
    .withColumn("current_unix_timestamp", unix_timestamp()) \
    .withColumn("processing_date", date_format(current_date(), "yyyy-MM-dd")) \
    .withColumn("processing_time", date_format(current_timestamp(), "HH:mm:ss"))
```

#### 2. Date Arithmetic and Calculations
```python
# Date arithmetic
date_calc_df = df \
    .withColumn("date_plus_30", date_add(col("order_date"), 30)) \
    .withColumn("date_minus_7", date_sub(col("order_date"), 7)) \
    .withColumn("days_since_order", datediff(current_date(), col("order_date"))) \
    .withColumn("months_between_dates", months_between(current_date(), col("order_date"))) \
    .withColumn("next_monday", next_day(col("order_date"), "monday")) \
    .withColumn("last_day_of_month", last_day(col("order_date")))
```

#### 3. Date Component Extraction
```python
# Extract date components
date_parts_df = df \
    .withColumn("year", year(col("order_date"))) \
    .withColumn("month", month(col("order_date"))) \
    .withColumn("day", dayofmonth(col("order_date"))) \
    .withColumn("quarter", quarter(col("order_date"))) \
    .withColumn("week_of_year", weekofyear(col("order_date"))) \
    .withColumn("day_of_week", dayofweek(col("order_date"))) \
    .withColumn("day_of_year", dayofyear(col("order_date")))

# Time component extraction
time_parts_df = df \
    .withColumn("hour", hour(col("timestamp"))) \
    .withColumn("minute", minute(col("timestamp"))) \
    .withColumn("second", second(col("timestamp")))
```

#### 4. Time Zone Operations
```python
# Time zone conversions
timezone_df = df \
    .withColumn("utc_timestamp", to_utc_timestamp(col("local_timestamp"), "PST")) \
    .withColumn("est_timestamp", from_utc_timestamp(col("utc_timestamp"), "EST")) \
    .withColumn("cet_timestamp", from_utc_timestamp(col("utc_timestamp"), "CET"))

# Handle different time zones in data
multi_tz_df = df \
    .withColumn("normalized_timestamp",
               when(col("timezone") == "PST", to_utc_timestamp(col("timestamp"), "PST"))
               .when(col("timezone") == "EST", to_utc_timestamp(col("timestamp"), "EST"))
               .when(col("timezone") == "CET", to_utc_timestamp(col("timestamp"), "CET"))
               .otherwise(col("timestamp")))
```

#### 5. Business Date Calculations
```python
# Business logic for dates
business_dates_df = df \
    .withColumn("is_weekend", dayofweek(col("date")).isin([1, 7])) \
    .withColumn("is_weekday", ~dayofweek(col("date")).isin([1, 7])) \
    .withColumn("business_day",
               when(col("is_weekend"), 
                    when(dayofweek(col("date")) == 1, date_add(col("date"), 1))  # Sunday → Monday
                    .otherwise(date_add(col("date"), 2)))                        # Saturday → Monday
               .otherwise(col("date"))) \
    .withColumn("quarter_start", 
               when(col("quarter") == 1, to_date(lit(f"{year(col('date'))}-01-01")))
               .when(col("quarter") == 2, to_date(lit(f"{year(col('date'))}-04-01")))
               .when(col("quarter") == 3, to_date(lit(f"{year(col('date'))}-07-01")))
               .otherwise(to_date(lit(f"{year(col('date'))}-10-01"))))
```

#### 6. Date Formatting and Parsing
```python
# Date formatting
formatted_dates_df = df \
    .withColumn("date_iso", date_format(col("date"), "yyyy-MM-dd")) \
    .withColumn("date_us", date_format(col("date"), "MM/dd/yyyy")) \
    .withColumn("date_verbose", date_format(col("date"), "MMMM dd, yyyy")) \
    .withColumn("timestamp_formatted", date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# Date parsing from strings
parsed_dates_df = df \
    .withColumn("parsed_date", to_date(col("date_string"), "MM/dd/yyyy")) \
    .withColumn("parsed_timestamp", to_timestamp(col("datetime_string"), "yyyy-MM-dd HH:mm:ss"))
```

**Example Usage:**
```python
# Sample order data with dates
order_data = [
    (1, "2024-01-15", "2024-01-15 14:30:00"),
    (2, "2024-02-20", "2024-02-20 09:15:30"),
    (3, "2024-03-10", "2024-03-10 18:45:15")
]

orders_df = spark.createDataFrame(order_data, ["order_id", "order_date", "order_timestamp"])
orders_df = orders_df \
    .withColumn("order_date", to_date(col("order_date"))) \
    .withColumn("order_timestamp", to_timestamp(col("order_timestamp")))

# Apply date/time transformations
datetime_df, date_ops_df, timezone_df, business_df = transformer.advanced_date_time_operations(orders_df)

# Results show:
# - Date arithmetic: days since order, future dates
# - Business logic: weekend handling, quarter calculations
# - Time zones: UTC conversion, regional times
```

### Date/Time Function Performance

| Function Category | Performance | Use Case | Notes |
|------------------|-------------|----------|--------|
| **Current Date/Time** | Fast | Auditing, processing timestamps | Computed once per partition |
| **Date Arithmetic** | Fast | Business calculations | Optimized operations |
| **Component Extraction** | Fast | Grouping, filtering | Very efficient |
| **Time Zone Conversion** | Medium | Global applications | Consider caching |
| **Complex Parsing** | Slow | String date formats | Pre-validate formats |

## Method: advanced_window_operations()

### Purpose
Advanced window function operations for sophisticated analytical processing.

### Window Function Categories

#### 1. Ranking and Analytical Functions
```python
from pyspark.sql.window import Window

# Define window specifications
partition_window = Window.partitionBy("category")
ordered_window = Window.partitionBy("category").orderBy("sales_amount")
date_ordered_window = Window.partitionBy("customer_id").orderBy("order_date")

# Ranking functions
ranking_df = df \
    .withColumn("row_number", row_number().over(ordered_window)) \
    .withColumn("rank", rank().over(ordered_window)) \
    .withColumn("dense_rank", dense_rank().over(ordered_window)) \
    .withColumn("percent_rank", percent_rank().over(ordered_window)) \
    .withColumn("ntile_quartile", ntile(4).over(ordered_window)) \
    .withColumn("cume_dist", cume_dist().over(ordered_window))
```

#### 2. Lag/Lead Operations
```python
# Access previous and next values
lag_lead_df = df \
    .withColumn("prev_amount", lag("amount", 1).over(date_ordered_window)) \
    .withColumn("next_amount", lead("amount", 1).over(date_ordered_window)) \
    .withColumn("prev_2_amount", lag("amount", 2).over(date_ordered_window)) \
    .withColumn("amount_change", col("amount") - col("prev_amount")) \
    .withColumn("amount_change_pct", 
               (col("amount") - col("prev_amount")) / col("prev_amount") * 100) \
    .withColumn("trend_direction",
               when(col("amount") > col("prev_amount"), "increasing")
               .when(col("amount") < col("prev_amount"), "decreasing")
               .otherwise("stable"))
```

#### 3. Moving Aggregations
```python
# Moving window calculations
moving_window_3 = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(-2, 0)
moving_window_7 = Window.partitionBy("customer_id").orderBy("order_date").rowsBetween(-6, 0)

moving_agg_df = df \
    .withColumn("moving_avg_3", avg("amount").over(moving_window_3)) \
    .withColumn("moving_sum_7", sum("amount").over(moving_window_7)) \
    .withColumn("moving_max_3", max("amount").over(moving_window_3)) \
    .withColumn("moving_min_3", min("amount").over(moving_window_3)) \
    .withColumn("moving_stddev_7", stddev("amount").over(moving_window_7))
```

#### 4. Running Aggregations
```python
# Running totals and cumulative calculations
running_window = Window.partitionBy("customer_id").orderBy("order_date") \
                       .rowsBetween(Window.unboundedPreceding, Window.currentRow)

running_agg_df = df \
    .withColumn("running_total", sum("amount").over(running_window)) \
    .withColumn("running_count", count("*").over(running_window)) \
    .withColumn("running_avg", avg("amount").over(running_window)) \
    .withColumn("running_max", max("amount").over(running_window)) \
    .withColumn("cumulative_pct", 
               col("running_total") / sum("amount").over(Window.partitionBy("customer_id")) * 100)
```

#### 5. First/Last Value Operations
```python
# First and last values in windows
first_last_window = Window.partitionBy("customer_id").orderBy("order_date") \
                          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

first_last_df = df \
    .withColumn("first_order_amount", first("amount").over(first_last_window)) \
    .withColumn("last_order_amount", last("amount").over(first_last_window)) \
    .withColumn("first_order_date", first("order_date").over(first_last_window)) \
    .withColumn("last_order_date", last("order_date").over(first_last_window)) \
    .withColumn("is_first_order", col("amount") == col("first_order_amount")) \
    .withColumn("is_last_order", col("amount") == col("last_order_amount"))
```

**Example Usage:**
```python
# Sample customer transaction data
transaction_data = [
    (1, "CUST001", "2024-01-01", 100),
    (2, "CUST001", "2024-01-05", 150),
    (3, "CUST001", "2024-01-10", 200),
    (4, "CUST002", "2024-01-02", 75),
    (5, "CUST002", "2024-01-07", 125)
]

transactions_df = spark.createDataFrame(transaction_data, 
    ["transaction_id", "customer_id", "order_date", "amount"])

# Apply window operations
ranking_df, lag_lead_df, moving_agg_df, first_last_df = transformer.advanced_window_operations(transactions_df)

# Results show:
# - Ranking: Customer transaction rankings
# - Lag/Lead: Previous and next transaction analysis
# - Moving averages: Trend analysis over time windows
# - Running totals: Cumulative customer spending
```

### Window Function Performance Optimization

| Window Type | Performance | Memory Usage | Use Case |
|-------------|-------------|--------------|----------|
| **Unbounded Windows** | Medium | High | Running totals, cumulative metrics |
| **Bounded Windows** | Fast | Medium | Moving averages, recent trends |
| **Range Windows** | Slow | High | Time-based windows |
| **Row Windows** | Fast | Low | Fixed-size sliding windows |

## Complete Usage Examples

### Example 1: Customer Data Enrichment Pipeline
```python
# Initialize transformer
transformer = SparkAdvancedTransformations("CustomerEnrichment")

# Raw customer data with various data quality issues
raw_customers = spark.read.table("raw_customer_data")

# Step 1: Clean and standardize names and emails
cleaned_df, extracted_df, string_ops_df, json_df = transformer.advanced_string_transformations(raw_customers)

# Step 2: Parse and standardize dates
datetime_df, date_ops_df, timezone_df, business_df = transformer.advanced_date_time_operations(cleaned_df)

# Step 3: Add complex data structures for analytics
array_df, exploded_df, map_df, struct_df = transformer.complex_data_type_operations(business_df)

# Step 4: Apply window functions for customer insights
final_df = transformer.advanced_window_operations(struct_df)
```

### Example 2: E-commerce Transaction Analysis
```python
# Transaction data with complex requirements
transactions_df = spark.read.table("ecommerce_transactions")

# Complex data type operations for product analysis
products_with_tags = transformer.complex_data_type_operations(transactions_df)

# Advanced date operations for seasonal analysis
seasonal_df = transformer.advanced_date_time_operations(products_with_tags)

# Window functions for customer behavior analysis
customer_insights = transformer.advanced_window_operations(seasonal_df)

# Results provide:
# - Product tag analysis through array operations
# - Seasonal patterns through date functions
# - Customer lifetime value through window functions
```

This comprehensive documentation covers all advanced transformation techniques with practical examples and performance considerations for complex ETL scenarios.
