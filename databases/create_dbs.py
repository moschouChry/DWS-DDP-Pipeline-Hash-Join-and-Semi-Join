import sqlite3
import random
import argparse
from datetime import datetime, timedelta
import numpy as np
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def random_date(start, end):
    """Generate a random datetime between `start` and `end`"""
    return start + timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

def create_orders_table(cursor, num_of_orders, start_date, min_order_amount, max_order_amount):
    cursor.execute('''
        DROP TABLE IF EXISTS Orders
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Orders (
            OrderID INTEGER PRIMARY KEY,
            CustomerID INTEGER,
            OrderDate TEXT,
            OrderAmount REAL
        )
    ''')
    orders = []
    order_ids = random.sample(range(1, num_of_orders + 1), num_of_orders)
    end_date = datetime.now()

    for order_id in order_ids:
        customer_id = random.randint(1, 1000)
        order_date = random_date(start_date, end_date).strftime('%Y-%m-%d %H:%M:%S')
        order_amount = round(random.uniform(min_order_amount, max_order_amount), 2)
        orders.append((order_id, customer_id, order_date, order_amount))

    cursor.executemany('INSERT INTO Orders VALUES (?, ?, ?, ?)', orders)
    logging.info(f"Orders table created successfully with {len(orders)} rows.")
    return orders

def create_shipments_table(cursor, orders, mean_shipments_per_order, std_shipments_per_order, max_shipping_days, min_shipment_cost, max_shipment_cost):
    cursor.execute('''
        DROP TABLE IF EXISTS Shipments
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Shipments (
            ShipmentID INTEGER PRIMARY KEY,
            OrderID INTEGER,
            ShipmentDate TEXT,
            ShipmentCost REAL,
            FOREIGN KEY(OrderID) REFERENCES Orders(OrderID)
        )
    ''')
    shipments = []
    shipment_ids = set()
    max_shipments = int(mean_shipments_per_order + 3 * std_shipments_per_order) * len(orders)  # Reasonable upper bound

    for order in orders:
        order_id = order[0]
        order_date = datetime.strptime(order[2], '%Y-%m-%d %H:%M:%S')
        num_of_shipments = max(1, int(random.gauss(mean_shipments_per_order, std_shipments_per_order)))

        for _ in range(num_of_shipments):
            shipment_id = random.randint(1, max_shipments)
            while shipment_id in shipment_ids:
                shipment_id = random.randint(1, max_shipments)
            shipment_ids.add(shipment_id)
            shipment_date = random_date(order_date, order_date + timedelta(days=max_shipping_days)).strftime('%Y-%m-%d %H:%M:%S')
            shipment_cost = round(random.uniform(min_shipment_cost, max_shipment_cost), 2)
            shipments.append((shipment_id, order_id, shipment_date, shipment_cost))

    cursor.executemany('INSERT INTO Shipments VALUES (?, ?, ?, ?)', shipments)
    logging.info(f"Shipments table created successfully with {len(shipments)} rows.")
    return shipments

def create_databases(args):
    os.makedirs(args.DBS_DIRECTORY, exist_ok=True)
    db1_path = os.path.join(args.DBS_DIRECTORY, args.DBS1_NAME)
    db2_path = os.path.join(args.DBS_DIRECTORY, args.DBS2_NAME)

    # Check if databases exist and delete them if they do
    if os.path.exists(db1_path):
        os.remove(db1_path)
        logging.info(f"Deleted existing database: {db1_path}")
    if os.path.exists(db2_path):
        os.remove(db2_path)
        logging.info(f"Deleted existing database: {db2_path}")

    conn1 = sqlite3.connect(db1_path)
    cursor1 = conn1.cursor()
    orders = create_orders_table(cursor1, args.NUM_OF_ORDERS, args.ORDERS_START_DATE, args.MIN_ORDER_AMOUNT, args.MAX_ORDER_AMOUNT)
    conn1.commit()
    conn1.close()

    conn2 = sqlite3.connect(db2_path)
    cursor2 = conn2.cursor()
    shipments = create_shipments_table(cursor2, orders, args.MEAN_SHIPMENTS_PER_ORDER, args.STD_SHIPMENTS_PER_ORDER, args.MAX_SHIPPING_DAYS, args.MIN_SHIPMENT_COST, args.MAX_SHIPMENT_COST)
    conn2.commit()
    conn2.close()

    # Log the size of the database files in MB
    db1_size = os.path.getsize(db1_path) / (1024 * 1024)
    db2_size = os.path.getsize(db2_path) / (1024 * 1024)
    logging.info(f"Size of {args.DBS1_NAME}: {db1_size:.2f} MB")
    logging.info(f"Size of {args.DBS2_NAME}: {db2_size:.2f} MB")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Create and populate two SQLite databases with orders and shipments.")
    parser.add_argument('--DBS_DIRECTORY', type=str, default='./databases', help="Directory to store the databases.")
    parser.add_argument('--DBS1_NAME', type=str, default='database1.db', help="Name of the first database.")
    parser.add_argument('--DBS2_NAME', type=str, default='database2.db', help="Name of the second database.")
    parser.add_argument('--NUM_OF_ORDERS', type=int, default=1000, help="Number of rows in the Orders table.")
    parser.add_argument('--ORDERS_START_DATE', type=lambda s: datetime.strptime(s, '%Y-%m-%d'), default=datetime(2024, 1, 1), help="Start date for orders (format: YYYY-MM-DD).")
    parser.add_argument('--MIN_ORDER_AMOUNT', type=float, default=0.1, help="Minimum amount for an order.")
    parser.add_argument('--MAX_ORDER_AMOUNT', type=float, default=100.0, help="Maximum amount for an order.")
    parser.add_argument('--MEAN_SHIPMENTS_PER_ORDER', type=float, default=5, help="Mean number of shipments per order.")
    parser.add_argument('--STD_SHIPMENTS_PER_ORDER', type=float, default=3, help="Standard deviation of shipments per order.")
    parser.add_argument('--MAX_SHIPPING_DAYS', type=int, default=20, help="Maximum number of days for shipment after order date.")
    parser.add_argument('--MIN_SHIPMENT_COST', type=float, default=0.5, help="Minimum cost for shipment.")
    parser.add_argument('--MAX_SHIPMENT_COST', type=float, default=5.0, help="Maximum cost for shipment.")

    args = parser.parse_args()

    assert args.MAX_ORDER_AMOUNT > args.MIN_ORDER_AMOUNT, "MAX_ORDER_AMOUNT must be greater than MIN_ORDER_AMOUNT."
    assert args.MEAN_SHIPMENTS_PER_ORDER > 0, "MEAN_SHIPMENTS_PER_ORDER must be greater than 0."
    assert args.MEAN_SHIPMENTS_PER_ORDER - args.STD_SHIPMENTS_PER_ORDER > 0, "MEAN_SHIPMENTS_PER_ORDER - STD_SHIPMENTS_PER_ORDER must be greater than 0."

    create_databases(args)
