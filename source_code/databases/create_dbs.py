import sqlite3
import random
from datetime import datetime, timedelta
import os
import argparse
import numpy as np
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_employees_table(cursor, num_of_employees, num_of_departments, random_seed):
    random.seed(random_seed)
    cursor.execute("DROP TABLE IF EXISTS Employees")
    cursor.execute("""
        CREATE TABLE Employees (
            EmployeeID INTEGER PRIMARY KEY,
            Department TEXT,
            Name TEXT,
            HireDate TEXT
        )
    """)

    departments_id = list(range(1, num_of_departments+1))
    departments_tag = list(['A','B','C','D','E'])
    departments = [tag+'_'+str(id) for tag in departments_tag for id in departments_id]

    employees = []
    for employee_id in range(1, num_of_employees + 1):
        department = random.choice(departments)
        name = f'Employee_{employee_id}'
        hire_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 364))
        employees.append((employee_id, department, name, hire_date.strftime('%Y-%m-%d')))

    cursor.executemany("INSERT INTO Employees (EmployeeID, Department, Name, HireDate) VALUES (?, ?, ?, ?)", employees)
    logging.info(f"Employees table created successfully with {len(employees)} rows.")
    
def create_projects_table(cursor, overlap_ratio, num_of_departments, avg_projects_per_department, std_projects_per_department, random_seed):
    random.seed(random_seed)
    np.random.seed(random_seed)
    cursor.execute("DROP TABLE IF EXISTS Projects")
    cursor.execute("""
        CREATE TABLE Projects (
            ProjectID INTEGER PRIMARY KEY,
            Department TEXT,
            StartDate TEXT,
            Funding INTEGER
        )
    """)

    departments_id = list(range(1, int((1/overlap_ratio))*num_of_departments))
    departments_tag = list(['A'])
    departments = [tag+'_'+str(id) for tag in departments_tag for id in departments_id]

    max_project_ids = int(len(departments)*(avg_projects_per_department+2*std_projects_per_department))
    project_ids = list(range(1,max_project_ids))
    projects = []
    for department in departments:
        num_projects = max(1, int(np.random.normal(avg_projects_per_department, std_projects_per_department)))
        for _ in range(num_projects):
            start_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 364))
            funding = random.randint(10,1000)*1000
            project_id = random.choice(project_ids)
            projects.append((project_id, department, start_date.strftime('%Y-%m-%d'), funding))
            project_ids.remove(project_id)

    cursor.executemany("INSERT INTO Projects (ProjectID, Department, StartDate, Funding) VALUES (?, ?, ?, ?)", projects)
    logging.info(f"Projects table created successfully with {len(projects)} rows.")
    
def main(args):
    if not os.path.exists(args.dbs_directory):
        os.makedirs(args.dbs_directory)
    
    db1_path = os.path.join(args.dbs_directory, args.dbs1_name)
    db2_path = os.path.join(args.dbs_directory, args.dbs2_name)
    
    # Check if databases exist and delete them if they do
    if os.path.exists(db1_path):
        os.remove(db1_path)
        logging.info(f"Deleted existing database: {db1_path}")
    if os.path.exists(db2_path):
        os.remove(db2_path)
        logging.info(f"Deleted existing database: {db2_path}")
    
    conn1 = sqlite3.connect(db1_path)
    cursor1 = conn1.cursor()
    create_projects_table(cursor1, args.overlap_ratio, args.num_of_departments, args.avg_projects_per_department, args.std_projects_per_department, args.random_seed)
    conn1.commit()
    
    conn2 = sqlite3.connect(db2_path)
    cursor2 = conn2.cursor()
    create_employees_table(cursor2, args.num_of_employees, args.num_of_departments, args.random_seed)
    conn2.commit()
    
    conn1.close()
    conn2.close()

    # Log the size of the database files in MB
    db1_size = os.path.getsize(db1_path) / (1024 * 1024)
    db2_size = os.path.getsize(db2_path) / (1024 * 1024)
    logging.info(f"Size of {args.dbs1_name}: {db1_size:.2f} MB")
    logging.info(f"Size of {args.dbs2_name}: {db2_size:.2f} MB")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create and populate Employees and Projects tables.")
    parser.add_argument('--dbs_directory', type=str, default='./databases', help="The directory to store the databases.")
    parser.add_argument('--dbs1_name', type=str, default='database1.db', help="The name of the first database.")
    parser.add_argument('--dbs2_name', type=str, default='database2.db', help="The name of the second database.")
    parser.add_argument('--num_of_employees', type=int, default=60000, help="The number of rows in the Employees table.")
    parser.add_argument('--overlap_ratio', type=float, default=0.25, help="The estimated overlap percentage on the join attribute.")
    parser.add_argument('--num_of_departments', type=int, default=100, help="The number of different departments per department tag.")
    parser.add_argument('--avg_projects_per_department', type=float, default=50.0, help="The average number of projects per department.")
    parser.add_argument('--std_projects_per_department', type=float, default=10.0, help="The standard deviation of the number of projects per department.")
    parser.add_argument('--random_seed', type=int, default=42, help="The seed for random number generation.")

    args = parser.parse_args()
    main(args)
