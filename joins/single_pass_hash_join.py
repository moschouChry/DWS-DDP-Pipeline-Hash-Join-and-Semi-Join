import sqlite3
import time
import os
import logging
import argparse
import csv
import sys
from datetime import datetime
import pickle

import sys
from collections.abc import Mapping, Container

# Setup logging to a file
def setup_logging(log_file):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
        logging.FileHandler(log_file ,mode='a'),
        logging.StreamHandler(sys.stdout)
    ])

def fetch_all_rows(cursor, table_name):
    cursor.execute(f"SELECT * FROM {table_name}")
    return cursor.fetchall()


def single_pass_hash_join(db1_path, db2_path, invert_join, max_days_diff):
    if invert_join:
        hash_db_path = db2_path
        probe_db_path = db1_path
        csv_file = "single_pass_hash_join_large_join_small.csv"
        result_type = "Single pass hash join (Large join Small)"
    else:
        hash_db_path = db1_path
        probe_db_path = db2_path
        csv_file = "single_pass_hash_join_small_join_large.csv"
        result_type = "Single pass hash join (Small join Large)"
    
    log_file = "results.log"
    setup_logging(log_file)
    logging.info(result_type)

    conn1 = sqlite3.connect(hash_db_path)
    conn2 = sqlite3.connect(probe_db_path)
    cursor1 = conn1.cursor()
    cursor2 = conn2.cursor()

    # Fetch the table names
    cursor1.execute("SELECT name FROM sqlite_master WHERE type='table'")
    table_name_1 = cursor1.fetchone()[0]
    cursor2.execute("SELECT name FROM sqlite_master WHERE type='table'")
    table_name_2 = cursor2.fetchone()[0]

    # Determine the join attribute and its index in each table
    if table_name_1.lower() == 'projects' and table_name_2.lower() == 'employees':
        join_index_table_1 = 1  # Department is the second column in Employees table
        join_index_table_2 = 1  # Department is the second column in Projects table
        timestamp_index_table_1 = 2  # HireDate is the fourth column in Employees table
        timestamp_index_table_2 = 3  # StartDate is the third column in Projects table
        columns = ['ProjectID', 'Department', 'StartDate', 'Funding', 'EmployeeID', 'Department', 'Name', 'HireDate']
    elif table_name_1.lower() == 'employees' and table_name_2.lower() == 'projects':
        join_index_table_1 = 1  # Department is the second column in Projects table
        join_index_table_2 = 1  # Department is the second column in Employees table
        timestamp_index_table_1 = 3  # StartDate is the third column in Projects table
        timestamp_index_table_2 = 2  # HireDate is the fourth column in Employees table
        columns = ['EmployeeID', 'Department', 'Name', 'HireDate', 'ProjectID', 'Department', 'StartDate', 'Funding']
    else:
        raise ValueError("Unexpected table names. Expected 'Employees' and 'Projects'.")

    # Fetch and calculate size for both tables
    table_1_rows = fetch_all_rows(cursor1, table_name_1)
    
    # Measure the start time for the build phase
    build_start_time = time.time()

    # Build the hash table on the main node
    logging.info("Building hash table on the main node...")
    hash_table = dict()
    for row in table_1_rows:
        key = row[join_index_table_1]
        if key not in hash_table:
            hash_table[key] = []
        hash_table[key].append(row)

    # Measure the end time for the build phase
    build_end_time = time.time()
    build_time = build_end_time - build_start_time
    logging.info(f"Build phase completed in {build_time:.4f} seconds")

    # Measure the start time for the probe phase
    probe_start_time = time.time()

    # Perform the probe phase
    logging.info(f"Performing probe phase with {table_name_2} table...")
    results = []
    first_record_time_logged = False
    first_record_time = 0.0
    cursor2.execute(f"SELECT * FROM {table_name_2}")
    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        for row in cursor2.fetchall():
            key = row[join_index_table_2]
            if key in hash_table:
                probe_timestamp = datetime.strptime(row[timestamp_index_table_2], '%Y-%m-%d')     
                for record in hash_table[key]:
                    # Check the timestamp difference
                    hash_timestamp = datetime.strptime(record[timestamp_index_table_1], '%Y-%m-%d')
                    #probe_timestamp = datetime.strptime(row[timestamp_index_table_2], '%Y-%m-%d')
                    if abs((probe_timestamp - hash_timestamp).days) <= max_days_diff:
                        result = record + row
                        results.append(result)
                        writer.writerow(result)
                        if not first_record_time_logged:
                            first_record_time = time.time()
                            first_record_time_logged = True

    # Measure the end time for the probe phase
    probe_end_time = time.time()
    probe_time = probe_end_time - probe_start_time
    logging.info(f"Probe phase completed in {probe_time:.4f} seconds")

    # Calculate the total execution time
    total_execution_time = build_time + probe_time

    hash_table_memory = sys.getsizeof(pickle.dumps(hash_table)) / ((1024)*(1024))  # Convert to MB

    logging.info(f"Join produced {len(results)} rows")
    # Calculate the time until the first record was written
    if first_record_time_logged:
        time_until_first_record = first_record_time - build_start_time
        logging.info(f"Time until the first record was extracted: {time_until_first_record:.4f} seconds")
    else:
        logging.info("No records were extracted.")
    logging.info(f"Hash table memory: {hash_table_memory:.4f} MB")
    logging.info(f"Total execution time: {total_execution_time:.4f} seconds")
    logging.info('-'*50)

    # Close the connections
    conn1.close()
    conn2.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Perform a Single Pass Hash Join between two SQLite databases.")
    parser.add_argument('--db1', type=str, default='./databases/database1.db', help="Path to the first database.")
    parser.add_argument('--db2', type=str, default='./databases/database2.db', help="Path to the second database.")
    parser.add_argument('--invert_join', type=bool, default=False, help='Instead of db1⨝db2 perform db2⨝db1. Default=False')
    parser.add_argument('--max_days_diff', type=int, default=10, help='Maximum allowed difference in days between timestamps for the join.')
    args = parser.parse_args()

    single_pass_hash_join(args.db1, args.db2, args.invert_join, args.max_days_diff)
