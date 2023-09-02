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
logging.info("Started executing the recipe code")

# Get the input of the recipe
input_dataset_name = get_input_names_for_role('input_dataset')[0]
input_dataset = dataiku.Dataset(input_dataset_name)

# Get the output of the recipe
output_folder_name = get_output_names_for_role('output_folder')[0]
output_folder = dataiku.Folder(output_folder_name)

# Get parameter values from the UI

partitioning_column = get_recipe_config()["partitioning_column"]
include_timestamp = get_recipe_config()["include_timestamp"]
clear_folder = get_recipe_config()["clear_folder"]

# clear folder before partitioning the datasets into CSV files)
if clear_folder:
    logging.info("clearing folder")
    output_folder.clear()

# get dataframe from dataset
input_data_df = input_dataset.get_dataframe()

# get partitions and write partitions to folder
def write_partitions(input_data_df):
    # get partition values
    partition_values = input_data_df[partitioning_column].unique()
    for partition in partition_values:
        df_1 = input_data_df[input_data_df[partitioning_column]==partition]
        # convert dataframe to csv file
        data = df_1.to_csv(index=False)
        #file name
        file_name = f"{partition}.csv"
        # write to non-local folder with .upload_stream
        logging.info(f"writing {file_name} to the folder")
        output_folder.upload_stream(file_name, data)

# get partitions and write partitions to folder with time stamps included
def write_partitions_timestamp(input_data_df):
    # get current timestamp
    current_time = datetime.datetime.now()
    current_time = current_time.strftime("%m-%d-%Y-%H-%M-%S")
    # get partition values 
    partition_values = input_data_df[partitioning_column].unique()
    for partition in partition_values:
        df_2 = input_data_df[input_data_df[partitioning_column]==partition]
        # convert dataframe to csv file
        data = df_2.to_csv(index=False)
        # create file name
        file_name = f"{partition}_{current_time}.csv"
        # write to non-local folder with .upload_stream
        logging.info(f"writing {file_name} to the folder")
        output_folder.upload_stream(file_name, data)

# partition the dataset and write partitions to the managed folder
if include_timestamp:
    write_partitions_timestamp(input_data_df)
else:
    write_partitions(input_data_df)

logging.info("Finished writing CSV files to the folder")



