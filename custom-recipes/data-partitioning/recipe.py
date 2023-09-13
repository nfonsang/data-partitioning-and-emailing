import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
import itertools
import datetime
import logging
import io

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
add_prefix = get_recipe_config().get("add_prefix", "")
file_format = get_recipe_config().get('file_format', "csv")
sheet_name = get_recipe_config().get('sheet_name', "Sheet1")

# clear folder before partitioning the datasets into CSV files)
if clear_folder:
    logging.info("clearing folder")
    output_folder.clear()

# get dataframe from dataset
input_data_df = input_dataset.get_dataframe()

# get partition dataframe and partition values from dataset
input_data_df = input_dataset.get_dataframe()



if partitioning_columns:
    partitioning_columns = partitioning_columns.split(",")
    partitioning_columns =[item.strip() for item in partitioning_columns]
    # get unique values of columns into a nested list
    unique_values_nested = []
    for col in partitioning_columns:
        unique_values = input_data_df[col].unique().tolist()
        unique_values_nested.append(unique_values)
        
    # get unique combinations of unique values 
    clean_unique_values = list(itertools.product(*unique_values_nested))
    
    # prepare strings in unique combination for the query
    clean_unique_values_2 = []
    for item in clean_unique_values:
        vs=[]
        for i in item:
            if isinstance(i, str):
                vs.append(f'"{i}"')
            else:
                vs.append(i)
        clean_unique_values_2.append(vs)
        
        # get a dictionary of key and values for various combinations
        key_values = [dict(zip(partitioning_columns, item)) for item in clean_unique_values_2]
        # get the queries
        queries = []
        for key_value in key_values:
            query = ' and '.join('{}=={}'.format(x,y) for x,y in key_value.items())
            queries.append(query)

        # get file names 
        file_names = []
        for item in clean_unique_values:
            item=list(item)
            if add_prefix:
                string_name = '_'.join(f'{c}' for c in item) + f'_{add_prefix}'
                file_names.append(string_name)
            else:
                string_name = '_'.join(f'{c}' for c in item)
                file_names.append(string_name)

        clean_file_names = [item.split() for item in file_names]
        clean_file_names = ['_'.join(f'{c}' for c in item) for item in clean_file_names]
       
        # get dataframe partitions and file names 
        dfs =[]
        final_file_names = []
            
        for i in range(len(queries)):
            df_part = input_data_df.query(queries[i])
            if columns_to_exclude:
                columns = [item.strip() for item in columns_to_exclude.split(",")]
                df_part = df_part.drop(columns, axis=1) 
            if len(df_part)>0:
                dfs.append(df_part)
                file_name = clean_file_names[i]
                final_file_names.append(file_name)



# write data partitions or entire data to folder
def write_partitions(df, partition):
    if partitioning_columns:
        if file_format=="excel":
            file_name = f"{partition}.xlsx"
            df=df.applymap(str)
            with io.BytesIO() as buf:
                df.to_excel(buf, sheet_name=sheet_name, encoding='utf-8', index = None, header = True)
                output_folder.upload_stream(file_name, buf.getvalue())
        else:
            data = df.to_csv(index=False)
            file_name = f"{partition}.csv" 
            logging.info(f"writing {file_name} to the folder")
            output_folder.upload_stream(file_name, data)

    
    # write entire dataframe
    else:
        if file_format=="excel":
            partition = input_dataset_name.split(".")[-1]
            file_name = f"{partition}_{add_prefix}.xlsx"
            df=df.applymap(str)
            with io.BytesIO() as buf:
                df.to_excel(buf, sheet_name=sheet_name, encoding='utf-8', index = None, header = True)
                output_folder.upload_stream(file_name, buf.getvalue())
        else:
    
            data = df.to_csv(index=False)
            partition = input_dataset_name.split(".")[-1]
            file_name = f"{partition}_{add_prefix}.csv"
            logging.info(f"writing {file_name} to the folder")
            output_folder.upload_stream(file_name, data)
    

# write partitions or entire data to folder with time stamps included
def write_partitions_timestamp(df, partition):
    # get current timestamp
    current_time = datetime.datetime.now()
    current_time = current_time.strftime("%m-%d-%Y-%H-%M-%S")
     
    if partitioning_columns:
        if file_format=="excel":
            file_name = f"{partition}_{current_time}.xlsx"
            df=df.applymap(str)
            with io.BytesIO() as buf:
                df.to_excel(buf, sheet_name=sheet_name, encoding='utf-8', index = None, header = True)
                output_folder.upload_stream(file_name, buf.getvalue())
        else:
            data = df.to_csv(index=False)
            file_name = f"{partition}_{current_time}.csv"
            logging.info(f"writing {file_name} to the folder")
            output_folder.upload_stream(file_name, data)

    
    # write entire dataframe
    else:
        if file_format=="excel":
            partition = input_dataset_name.split(".")[-1]
            file_name = f"{partition}_{add_prefix}_{current_time}.xlsx"
            df=df.applymap(str)
            with io.BytesIO() as buf:
                df.to_excel(buf, sheet_name=shee_tname, encoding='utf-8', index = None, header = True)
                output_folder.upload_stream(file_name, buf.getvalue())
        else:
    
            data = df.to_csv(index=False)
            partition = input_dataset_name.split(".")[-1]
            file_name = f"{partition}_{add_prefix}_{current_time}.csv"
            logging.info(f"writing {file_name} to the folder")
            output_folder.upload_stream(file_name, data)

if partitioning_columns:
    # partition the dataset and write partitions to the managed folder
    i=0
        
    for partition_df in dfs:
        partition = final_file_names[i]
        if include_timestamp:
            write_partitions_timestamp(partition_df, partition)
        else:
            write_partitions(partition_df, partition)
        i=i+1
        logging.info("Finished writing CSV files to the folder")
else:
    # write the entire data to the managed folder
    partition = input_dataset_name.split(".")[-1]
    if include_timestamp:
        write_partitions_timestamp(input_data_df, partition)
    else:
        write_partitions(input_data_df, partition)
    logging.info("Finished writing CSV files to the folder")
