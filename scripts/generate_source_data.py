from faker import Faker
from psycopg2.extras import execute_values
import psycopg2
import requests
import random
import time


# =====================================================
# CONFIG
# =====================================================

NUM_CUSTOMERS = 20_000
NUM_ORDERS = 100_000

fake = Faker()
Faker.seed(42)
random.seed(42)


# =====================================================
# START TIMER
# =====================================================

start_time = time.time()


# =====================================================
# GET PRODUCTS FROM API
# =====================================================

print("Fetching products from DummyJSON API...")

response = requests.get(
    "https://dummyjson.com/products?limit=0",
    timeout=30
)

response.raise_for_status()

products = response.json()["products"]

print(f"Fetched {len(products)} products from API")


# =====================================================
# POSTGRES CONNECTION
# =====================================================

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="retail_oltp",
    user="edl_user",
    password="edl_password"
)

cur = conn.cursor()


# =====================================================
# RESET
# =====================================================

print("Resetting source tables...")

cur.execute("""
TRUNCATE TABLE
    order_items,
    orders,
    customers
RESTART IDENTITY CASCADE;
""")

conn.commit()


# =====================================================
# CUSTOMERS
# =====================================================

print("Generating customers...")

customers = []

for _ in range(NUM_CUSTOMERS):

    customers.append((
        fake.first_name(),
        fake.last_name(),
        f"{fake.user_name()}_{fake.uuid4()}@example.com",
        fake.country(),
        fake.date_time_between(
            start_date="-2y",
            end_date="now"
        )
    ))


print("Bulk inserting customers...")

execute_values(
    cur,
    """
    INSERT INTO customers (
        first_name,
        last_name,
        email,
        country,
        created_at
    )
    VALUES %s
    """,
    customers,
    page_size=5000
)

conn.commit()

print(f"Inserted {NUM_CUSTOMERS:,} customers")


# =====================================================
# GENERATE ORDERS IN MEMORY
# =====================================================

print("Generating orders and order items...")

orders = []
order_items = []

order_item_id = 1

statuses = [
    "PAID",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED"
]


for order_id in range(1, NUM_ORDERS + 1):

    customer_id = random.randint(
        1,
        NUM_CUSTOMERS
    )

    order_timestamp = fake.date_time_between(
        start_date="-6M",
        end_date="now"
    )

    status = random.choice(statuses)

    number_of_items = random.randint(1, 4)

    total_amount = 0

    selected_items = []

    for _ in range(number_of_items):

        product = random.choice(products)

        product_id = product["id"]
        quantity = random.randint(1, 5)
        unit_price = float(product["price"])

        total_amount += quantity * unit_price

        selected_items.append((
            product_id,
            quantity,
            unit_price
        ))


    orders.append((
        order_id,
        customer_id,
        order_timestamp,
        status,
        round(total_amount, 2)
    ))


    for product_id, quantity, unit_price in selected_items:

        order_items.append((
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price
        ))

        order_item_id += 1


print(f"Generated {len(orders):,} orders")
print(f"Generated {len(order_items):,} order items")


# =====================================================
# BULK INSERT ORDERS
# =====================================================

print("Bulk inserting orders...")

execute_values(
    cur,
    """
    INSERT INTO orders (
        order_id,
        customer_id,
        order_timestamp,
        status,
        total_amount
    )
    VALUES %s
    """,
    orders,
    page_size=5000
)

conn.commit()


# =====================================================
# BULK INSERT ORDER ITEMS
# =====================================================

print("Bulk inserting order items...")

execute_values(
    cur,
    """
    INSERT INTO order_items (
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price
    )
    VALUES %s
    """,
    order_items,
    page_size=10000
)

conn.commit()


# =====================================================
# RESET POSTGRES SEQUENCES
# =====================================================

cur.execute("""
SELECT setval(
    pg_get_serial_sequence('orders', 'order_id'),
    (SELECT MAX(order_id) FROM orders)
);
""")

cur.execute("""
SELECT setval(
    pg_get_serial_sequence('order_items', 'order_item_id'),
    (SELECT MAX(order_item_id) FROM order_items)
);
""")

conn.commit()


# =====================================================
# DONE
# =====================================================

cur.close()
conn.close()

elapsed = time.time() - start_time

print()
print("======================================")
print("SOURCE DATA GENERATION COMPLETE")
print("======================================")
print(f"Customers   : {NUM_CUSTOMERS:,}")
print(f"Orders      : {NUM_ORDERS:,}")
print(f"Order Items : {len(order_items):,}")
print(f"Products    : {len(products):,} from API")
print(f"Time        : {elapsed:.2f} seconds")