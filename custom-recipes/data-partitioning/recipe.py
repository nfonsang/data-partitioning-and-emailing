import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
import itertools
import datetime
import logging
import io

# remove borders on header 
import pandas.io.formats.excel
pandas.io.formats.excel.ExcelFormatter.header_style = None


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

partitioning_columns = get_recipe_config().get("partitioning_column", "")
columns_to_exclude = get_recipe_config().get('columns_to_exclude', "")
include_timestamp = get_recipe_config().get("include_timestamp", None)
clear_folder = get_recipe_config().get("clear_folder", None)
add_suffix = get_recipe_config().get("add_suffix", "")
add_prefix =  get_recipe_config().get("add_prefix", "")

file_format = get_recipe_config().get('file_format', "csv")
sheet_name = get_recipe_config().get('sheet_name', "Sheet1")
start_col = get_recipe_config().get('start_col', 0)
start_row = get_recipe_config().get('start_row', 0)

# clear folder before writing new files
if clear_folder:
    for f in output_folder.list_paths_in_partition(''):
        output_folder.delete_path(f)
        logging.info(f"deleting {f}")
            
# get dataframe from dataset
input_data_df = input_dataset.get_dataframe()


# prepare prefix and suffix
prefix = f'{add_prefix}_' if add_prefix else ""
suffix = f'_{add_suffix}' if add_suffix else ""

# prepare columns to exclude 
if columns_to_exclude:
    columns_to_exclude = columns_to_exclude.split(",")
    columns_to_exclude =[item.strip() for item in columns_to_exclude]
    
# prepare partitioning columns
if partitioning_columns:
    partitioning_columns = partitioning_columns.split(",")
    partitioning_columns =[item.strip() for item in partitioning_columns]
    
    # get dataframe with unique values
    unique_data_df = input_data_df[partitioning_columns].value_counts().reset_index(name='count')
    
    # get partition data frames 
    dfs = []
    for i in range(len(unique_data_df)):
        unique_record = unique_data_df.iloc[i, 0:-1].values
        mask_df = input_data_df[partitioning_columns].isin(unique_record)
        partition_df = input_data_df[mask_df.sum(axis=1)==len(unique_record)]
        if columns_to_exclude:
            partition_df = partition_df.drop(columns_to_exclude, axis=1)
        
        dfs.append(partition_df)        
      
    # get file names 
    file_names = []
    for i in range(len(unique_data_df)):
        unique_record = unique_data_df.iloc[i, 0:-1].values
        clean_unique_record = [str(value).strip() for value in unique_record] # convert to string with no whitespaces
        clean_unique_record = ["_".join(value.split()) for value in clean_unique_record] # close any spaces between words
        clean_name = ['_'.join(f'{c}' for c in clean_unique_record)][0]
        file_name = prefix + clean_name + suffix
        file_names.append(file_name)

# write data partitions or entire data to folder function 

def write_partitions(df, partition):
    # write partitions
    if partitioning_columns:
        if file_format=="excel":
            file = f"{partition}.xlsx"
            df=df.applymap(str)
            with io.BytesIO() as buf:
                df.to_excel(buf, sheet_name=sheet_name, startrow=start_row, startcol=start_col, encoding='utf-8', index = None, header = True)
                output_folder.upload_stream(file, buf.getvalue())
        else:
            data = df.to_csv(index=False)
            file = f"{partition}.csv" 
            logging.info(f"writing {file} to the folder")
            output_folder.upload_stream(file, data)
    
    # write entire dataframe
    else:
        if columns_to_exclude:
            df = df.drop(columns_to_exclude, axis=1)
        if file_format=="excel":
            file = f"{partition}.xlsx"
            df=df.applymap(str)
            with io.BytesIO() as buf:
                df.to_excel(buf, sheet_name=sheet_name, startrow=start_row, startcol=start_col, encoding='utf-8', index = None, header = True)
                output_folder.upload_stream(file, buf.getvalue())
        else:
    
            data = df.to_csv(index=False)
            file = f"{partition}.csv"
            logging.info(f"writing {file} to the folder")
            output_folder.upload_stream(file, data)
    

# write partitions or entire data to folder with time stamps included

def write_partitions_timestamp(df, partition):
    # get current timestamp
    current_time = datetime.datetime.now()
    current_time = current_time.strftime("%m-%d-%Y-%H-%M-%S")
     
    if partitioning_columns:
        if file_format=="excel":
            file = f"{partition}_{current_time}.xlsx"
            df=df.applymap(str)
            with io.BytesIO() as buf:
                df.to_excel(buf, sheet_name=sheet_name, startcol=start_col, startrow=start_row, encoding='utf-8', index = None, header = True)
                output_folder.upload_stream(file, buf.getvalue())
        else:
            data = df.to_csv(index=False)
            file = f"{partition}_{current_time}.csv"
            logging.info(f"writing {file} to the folder")
            output_folder.upload_stream(file, data)

    # write entire dataframe
    else:
        if columns_to_exclude:
            df = df.drop(columns_to_exclude, axis=1)
        if file_format=="excel":
            file = f"{partition}_{current_time}.xlsx"
            df=df.applymap(str)
            with io.BytesIO() as buf:
                df.to_excel(buf, sheet_name=sheet_name,startcol=start_col, startrow=start_row, encoding='utf-8', index = None, header = True)
                output_folder.upload_stream(file, buf.getvalue())
        else:    
            data = df.to_csv(index=False)
            file = f"{partition}_{current_time}.csv"
            logging.info(f"writing {file} to the folder")
            output_folder.upload_stream(file, data)

if partitioning_columns:
    # partition the dataset and write partitions to the managed folder
    i=0
        
    for partition_df in dfs:
        partition = file_names[i]
        if include_timestamp:
            write_partitions_timestamp(partition_df, partition)
            logging.info(f"Finished writing {partition} file to the folder")

        else:
            write_partitions(partition_df, partition)
            logging.info(f"Finished writing {partition} file to the folder")
        i=i+1
else:
    # write the entire data to the managed folder
    data_name = input_dataset_name.split(".")[-1]
    partition = prefix + data_name + suffix
    
    if include_timestamp:
        write_partitions_timestamp(input_data_df, partition)
        logging.info(f"Finished writing {partition} file to the folder")
    else:
        write_partitions(input_data_df, partition)
        logging.info(f"Finished writing {partition} file to the folder")
        