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

def semi_join(db1_path, db2_path, invert_join, max_days_diff):
    if invert_join:
        driving_db_path = db1_path
        probed_db_path = db2_path
        csv_file = "semi_join_large_join_small.csv"
        result_type = "Semi-join (Large join Small)"
    else:
        driving_db_path = db2_path
        probed_db_path = db1_path
        csv_file = "semi_join_small_join_large.csv"
        result_type = "Semi-join (Small join Large)"
    
    log_file = "results.log"
    setup_logging(log_file)
    logging.info(result_type)

    conn1 = sqlite3.connect(driving_db_path)
    conn2 = sqlite3.connect(probed_db_path)
    cursor1 = conn1.cursor()
    cursor2 = conn2.cursor()

    # Fetch the table names
    cursor1.execute("SELECT name FROM sqlite_master WHERE type='table'")
    driving_table_name = cursor1.fetchone()[0]
    cursor2.execute("SELECT name FROM sqlite_master WHERE type='table'")
    probed_table_name = cursor2.fetchone()[0]

    # Determine the join attribute and its index in each table
    if driving_table_name.lower() == 'employees' and probed_table_name.lower() == 'projects':
        join_attribute = 'Department'
        driving_join_index = 1  # Department is the second column in Employees table
        probed_join_index = 1  # Department is the second column in Projects table
        driving_timestamp_index = 3  # HireDate is the fourth column in Employees table
        probed_timestamp_index = 2  # StartDate is the third column in Projects table
        columns = ['EmployeeID', 'Department', 'Name', 'HireDate', 'ProjectID', 'Department', 'StartDate', 'Funding']
    elif driving_table_name.lower() == 'projects' and probed_table_name.lower() == 'employees':
        join_attribute = 'Department'
        driving_join_index = 1  # Department is the second column in Projects table
        probed_join_index = 1  # Department is the second column in Employees table
        driving_timestamp_index = 2  # StartDate is the third column in Projects table
        probed_timestamp_index = 3  # HireDate is the fourth column in Employees table
        columns = ['ProjectID', 'Department', 'StartDate', 'Funding', 'EmployeeID', 'Department', 'Name', 'HireDate']
    else:
        raise ValueError("Unexpected table names. Expected 'Employees' and 'Projects'.")

    # Fetch distinct join attribute values from the driving table
    cursor1.execute(f"SELECT DISTINCT {join_attribute} FROM {driving_table_name}")
    join_values = [row[0] for row in cursor1.fetchall()]

    # Fetch rows from the probed table that match the join attribute values
    placeholder = ','.join(['?'] * len(join_values))
    query = f"SELECT * FROM {probed_table_name} WHERE {join_attribute} IN ({placeholder})"
    cursor2.execute(query, join_values)
    probed_rows = cursor2.fetchall()

    # Measure the start time for the join phase
    join_start_time = time.time()

    # Perform the semi-join
    logging.info(f"Performing semi-join between {driving_table_name} and {probed_table_name} tables...")
    results = []
    first_record_time_logged = False
    first_record_time = 0.0
    cursor1.execute(f"SELECT * FROM {driving_table_name}")
    with open(csv_file, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        for row1 in cursor1.fetchall():
            #driving_timestamp = datetime.strptime(row1[driving_timestamp_index], '%Y-%m-%d')
            for row2 in probed_rows:
                #probed_timestamp = datetime.strptime(row2[probed_timestamp_index], '%Y-%m-%d')
                if row1[driving_join_index] == row2[probed_join_index]:
                    # Check the timestamp difference
                    driving_timestamp = datetime.strptime(row1[driving_timestamp_index], '%Y-%m-%d')
                    probed_timestamp = datetime.strptime(row2[probed_timestamp_index], '%Y-%m-%d')
                    if abs((probed_timestamp - driving_timestamp).days) <= max_days_diff:
                        result = row1 + row2
                        results.append(result)
                        writer.writerow(result)
                        if not first_record_time_logged:
                            first_record_time = time.time()
                            first_record_time_logged = True

    # Measure the end time for the join phase
    join_end_time = time.time()
    join_time = join_end_time - join_start_time
    logging.info(f"Join phase completed in {join_time:.4f} seconds")

    # Calculate the total execution time
    total_execution_time = join_time
    logging.info(f"Total execution time: {total_execution_time:.4f} seconds")

    # Calculate the time until the first record was written
    if first_record_time_logged:
        time_until_first_record = first_record_time - join_start_time
        logging.info(f"Time until the first record was written to the CSV: {time_until_first_record:.4f} seconds")
    else:
        logging.info("No records were written to the CSV.")

    join_values_memory = sys.getsizeof(pickle.dumps(join_values)) / ((1024)*(1024))  # Convert to MB
    probed_rows_memory = sys.getsizeof(pickle.dumps(probed_rows)) / ((1024)*(1024))  # Convert to MB

    logging.info(f"Join produced {len(results)} rows")
    # Calculate the time until the first record was written
    if first_record_time_logged:
        time_until_first_record = first_record_time - join_start_time
        logging.info(f"Time until the first record was extracted: {time_until_first_record:.4f} seconds")
    else:
        logging.info("No records were extracted.")
    logging.info(f"Join values memory: {join_values_memory:.4f} MB")
    logging.info(f"Probed rows memory: {probed_rows_memory:.4f} MB")
    logging.info(f"Total memory required: {join_values_memory+probed_rows_memory:.4f} MB")
    logging.info(f"Total execution time: {total_execution_time:.4f} seconds")
    logging.info('-'*50)

    # Close the connections
    conn1.close()
    conn2.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Perform a Semi-Join between two SQLite databases.")
    parser.add_argument('--db1', type=str, default='./databases/database1.db', help="Path to the first database.")
    parser.add_argument('--db2', type=str, default='./databases/database2.db', help="Path to the second database.")
    parser.add_argument('--invert_join', type=bool, default=False, help='Instead of db1⨝db2 perform db2⨝db1. Default=False')
    parser.add_argument('--max_days_diff', type=int, default=10, help='Maximum allowed difference in days between timestamps for the join.')
    args = parser.parse_args()

    semi_join(args.db1, args.db2, args.invert_join, args.max_days_diff)
