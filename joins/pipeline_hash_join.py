import sqlite3
import time
import os
import logging
import argparse
import csv
import sys
from datetime import datetime
import pickle

# Setup logging to a file
def setup_logging(log_file):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
        logging.FileHandler(log_file ,mode='a'),
        logging.StreamHandler(sys.stdout)
    ])

def fetch_all_rows(cursor, table_name):
    cursor.execute(f"SELECT * FROM {table_name}")
    return cursor.fetchall()

def pipelined_hash_join(db1_path, db2_path, invert_join, max_days_diff):
    if invert_join:
        hash_db_path = db2_path
        probe_db_path = db1_path
        csv_file = "pipelined_hash_join_large_join_small.csv"
        result_type = "Pipelined hash join (Large join Small)"
    else:
        hash_db_path = db1_path
        probe_db_path = db2_path
        csv_file = "pipelined_hash_join_small_join_large.csv"
        result_type = "Pipelined hash join (Small join Large)"
    
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
        join_index_table_1 = 1  # Department is the second column in Projects table
        join_index_table_2 = 1  # Department is the second column in Employees table
        timestamp_index_table_1 = 2  # HireDate is the fourth column in Projects table
        timestamp_index_table_2 = 3  # StartDate is the third column in Employees table
        columns = ['ProjectID', 'Department', 'StartDate', 'Funding', 'EmployeeID', 'Department', 'Name', 'HireDate']
    elif table_name_1.lower() == 'employees' and table_name_2.lower() == 'projects':
        join_index_table_1 = 1  # Department is the second column in Employees table
        join_index_table_2 = 1  # Department is the second column in Projects table
        timestamp_index_table_1 = 3  # StartDate is the third column in Employees table
        timestamp_index_table_2 = 2  # HireDate is the fourth column in Projects table
        columns = ['EmployeeID', 'Department', 'Name', 'HireDate', 'ProjectID', 'Department', 'StartDate', 'Funding']
    else:
        raise ValueError("Unexpected table names. Expected 'Employees' and 'Projects'.")

    # Measure the start time for the build phase
    build_start_time = time.time()

    # Initialize hash tables
    hash_table1 = {}
    hash_table2 = {}

    # Set to keep track of written pairs to avoid duplicates
    written_pairs = set()

    # Measure the start time for the probe phase
    probe_start_time = time.time()

    # Perform the probe phase
    logging.info(f"Performing probe phase with {table_name_1} and {table_name_2} tables...")
    results = []
    first_record_time_logged = False
    first_record_time = 0.0
    cursor1.execute(f"SELECT * FROM {table_name_1}")
    cursor2.execute(f"SELECT * FROM {table_name_2}")
    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        
        while True:
            row1 = cursor1.fetchone()
            if row1:
                key = row1[join_index_table_1]
                if key in hash_table2:
                    timestamp_1 = datetime.strptime(row1[timestamp_index_table_1], '%Y-%m-%d')
                    for record in hash_table2[key]:
                        # Check the timestamp difference
                        #timestamp_1 = datetime.strptime(row1[timestamp_index_table_1], '%Y-%m-%d')
                        timestamp_2 = datetime.strptime(record[timestamp_index_table_2], '%Y-%m-%d')
                        if abs((timestamp_1 - timestamp_2).days) <= max_days_diff:
                            result = row1 + record
                            if (row1, record) not in written_pairs:
                                results.append(result)
                                writer.writerow(result)
                                written_pairs.add((row1, record))
                            if not first_record_time_logged:
                                first_record_time = time.time()
                                first_record_time_logged = True
                if key not in hash_table1:
                    hash_table1[key] = []
                hash_table1[key].append(row1)
            
            row2 = cursor2.fetchone()
            if row2:
                key = row2[join_index_table_2]
                if key in hash_table1:
                    timestamp_2 = datetime.strptime(row2[timestamp_index_table_2], '%Y-%m-%d')
                    for record in hash_table1[key]:
                        # Check the timestamp difference
                        timestamp_1 = datetime.strptime(record[timestamp_index_table_1], '%Y-%m-%d')
                        #timestamp_2 = datetime.strptime(row2[timestamp_index_table_2], '%Y-%m-%d')
                        if abs((timestamp_1 - timestamp_2).days) <= max_days_diff:
                            result = record + row2
                            if (record, row2) not in written_pairs:
                                results.append(result)
                                writer.writerow(result)
                                written_pairs.add((record, row2))
                            if not first_record_time_logged:
                                first_record_time = time.time()
                                first_record_time_logged = True
                if key not in hash_table2:
                    hash_table2[key] = []
                hash_table2[key].append(row2)
            
            if row1 is None and row2 is None:
                break

    # Measure the end time for the probe phase
    probe_end_time = time.time()
    probe_time = probe_end_time - probe_start_time
    logging.info(f"Probe phase completed in {probe_time:.4f} seconds")

    # Calculate the total execution time
    total_execution_time = probe_time

    hash_table_memory_1 = sys.getsizeof(pickle.dumps(hash_table1)) / ((1024)*(1024))  # Convert to MB
    hash_table_memory_2 = sys.getsizeof(pickle.dumps(hash_table2)) / ((1024)*(1024))  # Convert to MB

    logging.info(f"Join produced {len(results)} rows")
    # Calculate the time until the first record was written
    if first_record_time_logged:
        time_until_first_record = first_record_time - build_start_time
        logging.info(f"Time until the first record was extracted: {time_until_first_record:.4f} seconds")
    else:
        logging.info("No records were extracted.")
    logging.info(f"Hash table 1 memory: {hash_table_memory_1:.4f} MB")
    logging.info(f"Hash table 2 memory: {hash_table_memory_2:.4f} MB")
    logging.info(f"Total hash table memory: {(hash_table_memory_1+hash_table_memory_2):.4f} MB")
    logging.info(f"Total execution time: {total_execution_time:.4f} seconds")
    logging.info('-'*50)

    # Close the connections
    conn1.close()
    conn2.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Perform a Pipelined Hash Join between two SQLite databases.")
    parser.add_argument('--db1', type=str, default='./databases/database1.db', help="Path to the first database.")
    parser.add_argument('--db2', type=str, default='./databases/database2.db', help="Path to the second database.")
    parser.add_argument('--invert_join', type=bool, default=False, help='Instead of db1⨝db2 perform db2⨝db1. Default=False')
    parser.add_argument('--max_days_diff', type=int, default=10, help='Maximum allowed difference in days between timestamps for the join.')
    args = parser.parse_args()

    pipelined_hash_join(args.db1, args.db2, args.invert_join, args.max_days_diff)
