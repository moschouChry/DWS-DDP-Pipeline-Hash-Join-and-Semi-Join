import subprocess
import glob
import pandas as pd

# Function to execute a script
def execute_script(script_name, *args):
    cmd = ['python', script_name] + list(args)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error executing {script_name} with args {args}: {result.stderr}")
    else:
        print(f"Executed {script_name} with args {args}")

# Execute create_dbs.py first
execute_script('databases/create_dbs.py')

# Execute the other scripts in the specified order
execute_script('joins/single_pass_hash_join.py')
execute_script('joins/single_pass_hash_join.py', '--invert_join=True')
execute_script('joins/pipeline_hash_join.py')
execute_script('joins/pipeline_hash_join.py', '--invert_join=True')
execute_script('joins/semi_join.py')
execute_script('joins/semi_join.py', '--invert_join=True')

# Function to read and normalize CSV files
def read_and_normalize_csv(filepath):
    df = pd.read_csv(filepath)
    df = df.reindex(sorted(df.columns), axis=1)  # Sort the columns
    return df.sort_values(by=df.columns.tolist()).reset_index(drop=True)


# Find all CSV files in the current directory
csv_files = glob.glob('*.csv')

# Read and normalize all CSV files
dataframes = [read_and_normalize_csv(file) for file in csv_files]

# Check if all dataframes are equal
assert all(df.equals(dataframes[0]) for df in dataframes), "CSV files contain different data"

print("All CSV files contain the same data.")
