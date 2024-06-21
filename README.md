
# Pipeline Hash Join and Semi-Join

This repository contains the implementation of various join methods for distributed data processing, specifically focusing on hash join and semi-join techniques. The methods implemented include single pass hash join, pipelined hash join, and semi-join, each with options to invert the join direction. The project is a part of the Distributed Data Processing course at Aristotle University of Thessaloniki.

## Contents

- `main.py`: The main script to create databases, perform joins, and log results.
- `Dockerfile`: Docker configuration file to build and run the project in a containerized environment.
- `requirements.txt`: List of Python dependencies required to run the project.
- `databases/create_dbs.py`: Script to create SQLite databases for the project.
- `joins/pipeline_hash_join.py`: Implementation of the pipeline hash join method.
- `joins/semi_join.py`: Implementation of the semi-join method.
- `joins/single_pass_hash_join.py`: Implementation of the single pass hash join method.


## Prerequisites

- Docker installed on your machine.
- Python (if running outside Docker).
- SQLite (if running outside Docker).

## Installation

1. **Navigate to the source_code repository:**

    ```sh
    cd source_code
    ```

2. **Build the Docker image:**

    ```sh
    docker build -t join_methods .
    ```

## Usage

### Running with Docker

1. **Build the Docker image (if not already built):**

    ```sh
    docker build -t join_methods .
    ```

2. **Run the Docker container:**

    ```sh
    docker run --rm -v $(pwd):/app join_methods
    ```

    The above command mounts the current directory to the `/app` directory inside the container and executes the `main.py` script.

### Running Locally

1. **Install Python dependencies:**

    ```sh
    pip install -r requirements.txt
    ```

2. **Run the main script:**

    ```sh
    python main.py
    ```

## Configuration

The script allows you to define several parameters for dataset creation and join operations:

- `dbs directory`: The directory to store the databases. (Default: `./databases`)
- `num of employees`: The number of rows in the Employees table. (Default: 60000)
- `overlap ratio`: The estimated overlap percentage on the join attribute. (Default: 0.25)
- `num of departments`: The number of different departments per department tag. (Default: 100)
- `avg projects per department`: The average number of projects per department. (Default: 50.0)
- `std projects per department`: The standard deviation of the number of projects per department. (Default: 10.0)
- `max days`: The maximum allowable difference in days between timestamp values. (Default: 10 days)

## Results

The results of the join operations, including metrics like build phase time, probe phase time, memory usage, and execution time, are logged in a file named `result.log`.
