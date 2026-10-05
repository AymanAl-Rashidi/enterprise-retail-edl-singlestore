import os
from dotenv import load_dotenv
import pymysql
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("SINGLESTORE_HOST", "localhost"),
    "port": int(os.getenv("SINGLESTORE_PORT", "3306")),
    "user": os.getenv("SINGLESTORE_USER"),
    "password": os.getenv("SINGLESTORE_PASSWORD"),
    "database": os.getenv("SINGLESTORE_DATABASE"),
    "autocommit": True
}


connection = pymysql.connect(**DB_CONFIG)


try:
    with connection.cursor() as cursor:

        # ==========================================
        # DAILY SALES
        # ==========================================

        cursor.execute("TRUNCATE TABLE daily_sales")

        cursor.execute(
            """
            LOAD DATA INFILE '/data/gold/daily_sales.parquet'
            INTO TABLE daily_sales
            (
                @sales_date <- `sales_date`,
                `total_orders` <- `total_orders`,
                `unique_customers` <- `unique_customers`,
                `total_revenue` <- `total_revenue`,
                `avg_order_value` <- `avg_order_value`,
                @gold_ts <- `_gold_processed_at`
            )
            FORMAT PARQUET
            SET
                sales_date =
                    DATE_ADD(
                        '1970-01-01',
                        INTERVAL @sales_date DAY
                    ),

                gold_processed_at =
                    DATE_ADD(
                        '1970-01-01',
                        INTERVAL @gold_ts MICROSECOND
                    )
            """
        )

        print("daily_sales loaded")


        # ==========================================
        # CUSTOMER 360
        # ==========================================

        cursor.execute("TRUNCATE TABLE customer_360")

        cursor.execute(
            """
            LOAD DATA INFILE '/data/gold/customer_360.parquet'
            INTO TABLE customer_360
            (
                `customer_id` <- `customer_id`,
                `first_name` <- `first_name`,
                `last_name` <- `last_name`,
                `email` <- `email`,
                `country` <- `country`,
                @created_at <- `created_at`,
                `total_orders` <- `total_orders`,
                `total_spent` <- `total_spent`,
                `avg_order_value` <- `avg_order_value`,
                @first_order_at <- `first_order_at`,
                @last_order_at <- `last_order_at`,
                @gold_ts <- `_gold_processed_at`
            )
            FORMAT PARQUET
            SET
                created_at =
                    DATE_ADD(
                        '1970-01-01',
                        INTERVAL @created_at MICROSECOND
                    ),

                first_order_at =
                    DATE_ADD(
                        '1970-01-01',
                        INTERVAL @first_order_at MICROSECOND
                    ),

                last_order_at =
                    DATE_ADD(
                        '1970-01-01',
                        INTERVAL @last_order_at MICROSECOND
                    ),

                gold_processed_at =
                    DATE_ADD(
                        '1970-01-01',
                        INTERVAL @gold_ts MICROSECOND
                    )
            """
        )

        print("customer_360 loaded")


        # ==========================================
        # PRODUCT PERFORMANCE
        # ==========================================

        cursor.execute(
            "TRUNCATE TABLE product_performance"
        )

        cursor.execute(
            """
            LOAD DATA INFILE '/data/gold/product_performance.parquet'
            INTO TABLE product_performance
            (
                `product_id` <- `product_id`,
                `product_name` <- `product_name`,
                `category` <- `category`,
                `unit_price` <- `unit_price`,
                `stock` <- `stock`,
                `rating` <- `rating`,
                `total_orders` <- `total_orders`,
                `units_sold` <- `units_sold`,
                `total_revenue` <- `total_revenue`,
                @gold_ts <- `_gold_processed_at`
            )
            FORMAT PARQUET
            SET
                gold_processed_at =
                    DATE_ADD(
                        '1970-01-01',
                        INTERVAL @gold_ts MICROSECOND
                    )
            """
        )

        print("product_performance loaded")


        # ==========================================
        # VALIDATION
        # ==========================================

        for table in [
            "daily_sales",
            "customer_360",
            "product_performance"
        ]:
            cursor.execute(
                f"SELECT COUNT(*) FROM {table}"
            )

            count = cursor.fetchone()[0]

            print(
                f"{table}: {count:,} rows"
            )

    print()
    print("================================")
    print("GOLD -> SINGLESTORE COMPLETE")
    print("================================")

finally:
    connection.close()