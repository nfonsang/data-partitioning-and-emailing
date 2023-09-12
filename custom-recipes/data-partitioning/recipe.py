import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
import datetime
import logging


from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config

# set logging configurations
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logging.info("Start executing the recipe code")

# Get the input of the recipe
input_dataset_name = get_input_names_for_role('input_dataset')[0]
input_dataset = dataiku.Dataset(input_dataset_name)

# Get the output of the recipe
output_folder_name = get_output_names_for_role('output_folder')[0]
output_folder = dataiku.Folder(output_folder_name)

# Get parameter values from the UI

partitioning_column = get_recipe_config().get("partitioning_column", "")
columns_to_exclude = get_recipe_config().get('columns_to_exclude', "")
include_timestamp = get_recipe_config().get("include_timestamp", None)
clear_folder = get_recipe_config().get("clear_folder", None)


# clear folder before partitioning the datasets into CSV files)
if clear_folder:
    logging.info("clearing folder")
    output_folder.clear()

# get dataframe from dataset
input_data_df = input_dataset.get_dataframe()

# get partition dataframe and partition values from dataset
input_data_df = input_dataset.get_dataframe()

partitioning_columns=partitioning_columns.split(",")

if partitioning_column:
    partition_values = input_data_df[partitioning_column].unique()
    partition_dfs = []
    for partition in partition_values:
        partition_df = input_data_df[input_data_df[partitioning_column]==partition]
        if columns_to_exclude:
            columns = [item.strip() for item in columns_to_exclude.split(",")]
            partition_df = partition_df.drop(columns, axis=1)
        partition_dfs.append(partition_df)
else:
    if columns_to_exclude:
        columns = [item.strip() for item in columns_to_exclude.split(",")]
        input_data_df = input_data_df.drop(columns, axis=1)

# write data partitions or entire data to folder
def write_partitions(df, partition):
    if partitioning_column:
        data = df.to_csv(index=False)
        file_name = f"{partition}.csv"
        logging.info(f"writing {file_name} to the folder")
        output_folder.upload_stream(file_name, data)
    else:
        # write entire dataframe
        data = df.to_csv(index=False)
        partition = input_dataset_name.split(".")[-1]
        file_name = f"{partition}.csv"
        logging.info(f"writing {file_name} to the folder")
        output_folder.upload_stream(file_name, data)
    

# write partitions or entire data to folder with time stamps included


# write data partitions or entire data to folder
def write_partitions(df, partition):
    if partitioning_column:
        data = df.to_csv(index=False)
        file_name = f"{partition}.csv"
        logging.info(f"writing {file_name} to the folder")
        output_folder.upload_stream(file_name, data)
    else:
        # write entire dataframe
        data = df.to_csv(index=False)
        partition = input_dataset_name.split(".")[-1]
        file_name = f"{partition}.csv"
        logging.info(f"writing {file_name} to the folder")
        output_folder.upload_stream(file_name, data)
    
    
# write partitions or entire data to folder with time stamps included
def write_partitions_timestamp(df, partition):
    # get current timestamp
    current_time = datetime.datetime.now()
    current_time = current_time.strftime("%m-%d-%Y-%H-%M-%S")
 
    if partitioning_column:
        data = df.to_csv(index=False)
        file_name = f"{partition}_{current_time}.csv"
        logging.info(f"writing {file_name} to the folder")
        output_folder.upload_stream(file_name, data)
    else:
        # write entire data to managed folder
        data = input_data_df.to_csv(index=False)
        partition = input_dataset_name.split(".")[-1]
        file_name = f"{partition}_{current_time}.csv"
        logging.info(f"writing {file_name} to the folder")
        output_folder.upload_stream(file_name, data)


if partitioning_column:
    # partition the dataset and write partitions to the managed folder
    i=0
    for partition_df in partition_dfs:
        partition = partition_values[i]
        if include_timestamp:
            write_partitions_timestamp(partition_df, partition)
        else:
            write_partitions(partition_df, partition)
        i=i+1
        logging.info("Finished writing CSV files to the folder")
else:
    # write write the entire data to the managed folder
    partition = input_dataset_name.split(".")[-1]
    if include_timestamp:
        write_partitions_timestamp(input_data_df, partition)
    else:
        write_partitions(input_data_df, partition)
    logging.info("Finished writing CSV files to the folder")
