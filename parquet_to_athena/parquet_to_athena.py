#!/usr/bin/env python3
import pyarrow.parquet as pq
from query_athena import query_athena
import s3fs
import sys
import json
from argparse import ArgumentParser
s3 = s3fs.S3FileSystem()

DATA_TYPE = {"STRING": "STRING", "INT32": "INTEGER", "INT64": "INTEGER"}
CREATE_TABLE_QUERY = """
CREATE EXTERNAL TABLE {}.{} (
{}
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '{}'
"""
CREATE_TABLE_QUERY_WITH_PARTITIONS = """
CREATE EXTERNAL TABLE {}.{} (
{}
)
PARTITIONED BY (
{}
)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  '{}'

"""

def parquet_to_athena(file_path, location, database,  table, partition, create_table=False, workgroup='primary', region='us-west-2'):
    """
    Desc: This function reads the schema of a parquet file and generates the command for creating table in athena and/or creates the table
    Args:
      file_path: Parquet file path in s3 to derive the schema from [Type: String]
      location: s3 location of the folder which athena table will use [Type: string]
      database: Name of the database in athena [Type: String]
      table: Name of the table in athena [Type: String]
      partition: Object having partition name as key & datatype as value [Type: Object]
      create_table: pass staging s3 path if table needs to be created
      workgroup: pass workgroup for athena if table needs to be created
      region: pass region if table needs to be created
    Returns:
      Prints command for creating table in athena.
      If required it creates athena table

    """
    dataframe = pq.ParquetDataset(file_path, filesystem=s3)
    column_str = ''
    for col in range(len(dataframe.schema)):
        name = (dataframe.schema[col].name).lower()
        physical_col_type = dataframe.schema[col].physical_type
        logical_col_type = dataframe.schema[col].logical_type.type
        if not logical_col_type=="NONE":
            if logical_col_type=="DECIMAL":
                precision = dataframe.schema[col].precision
                scale = dataframe.schema[col].scale
                dtype = f"{logical_col_type}({precision}, {scale})"
            else:
                dtype = DATA_TYPE[logical_col_type]
        else:
            dtype = DATA_TYPE[physical_col_type]
        if col == len(dataframe.schema)-1:
            column_str += f"{name} {dtype}"
        else:
            column_str += f"{name} {dtype},\n"

    query = CREATE_TABLE_QUERY.format(database, table, column_str, location)
    if partition:
      partition_str = ''
      counter = 0
      for k,v in partition.items():
        counter+=1
        if len(partition)==1 or len(partition)==counter:
          partition_str +=  f"{k} {v}"
        else: 
          partition_str += f"{k} {v},\n"
      query = CREATE_TABLE_QUERY_WITH_PARTITIONS.format(database, table, column_str, partition_str, location )

    
    print(query)
    if create_table:
      status = query_athena(query, create_table, True, True, workgroup, region)
      return status 

def main():
    parser = ArgumentParser(description="This script reads s3 parquet file schema & generates table schema in athena")
    parser.add_argument("config", type=str, help="Provide the config json file which contains file_path, location, database, table , partition [optional]")
    args = parser.parse_args()
    data = json.load(open(args.config))
    file_path = data["file_path"]
    location = data["location"]
    database = data["database"]
    table = data["table"]
    partition = data["partition"]
    create_table = data["create_table"]
    workgroup = data["workgroup"]
    region = data["region"]
    status = parquet_to_athena(file_path, location,database,table, partition, create_table, workgroup, region)
    if status == False:
       sys.exit(2)
    else:
        pass

if __name__ == "__main__":
   main()

    
