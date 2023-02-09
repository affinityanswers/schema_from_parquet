# parquet_to_athena
---
This is a Python package which reads the schema of a parquet file & generates the create table command for Athena. It also creates an athena table if required 
## Installation

This package runs with python 3.7 or higher version than that.
Using the package manager(pip), you can install this package from the git as shown below.  
Run this command in your home directory for installation.

``` 
pip3.7 install git+https://github.com/affinityanswers/schema_from_parquet.git

```

## Usage

You can import the package and functions to verify if it is installed properly.

```python 
from parquet_to_athena import parquet_to_athena
```

```python 

Desc: This function reads the schema of a parquet file and generates the command for creating table in athena and/or creates the table
    Args:
      file_path: Parquet file path in s3 to derive the schema from [Type: String]
      location: s3 location of the folder which athena table will use [Type: string]
      database: Name of the database in athena [Type: String]
      table: Name of the table in athena [Type: String]
      partition: Object having partition name as key & datatype as value [Type: Object]
      create_table: [optional] pass staging s3 path when table needs to be created 
      workgroup: [optional] pass workgroup if any. Default primary 
      region: [optional] pass region. Default us-west-2 
    Returns:
      Prints command for creating table in athena.
      If required it creates athena table

```

## Example

```python
from parquet_to_athena import parquet_to_athena
  
file_path = "s3://bucket/sample.parquet"
location = "s3://bucket/output"
database = "test"
table = "test_table"
partition = {"key": "datatype"}
create_table = "s3://bucket/staging"
workgroup = "XYZ"
reqion = "us-west-2"
status = parquet_to_athena(file_path, location, database, table, partition, create_table, workgroup, reqion)
```
