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

partitioning_columns = get_recipe_config().get("partitioning_column", None)
sheet_name = get_recipe_config().get("sheet_name", "Sheet1")
use_partition_value_for_sheetname = get_recipe_config().get('use_partition_value_for_sheetname', None)
columns_to_exclude = get_recipe_config().get('columns_to_exclude', "")
file_name = get_recipe_config().get('file_name', None)
existing_file = get_recipe_config().get('existing_file', None) 
use_existing_file = get_recipe_config().get('use_existing_file', None)

start_col = get_recipe_config().get('start_col', 0)
start_row = get_recipe_config().get('start_row', 0)
include_timestamp = get_recipe_config().get("include_timestamp", None)
clear_folder = get_recipe_config().get("clear_folder", None)

# clear folder before writing new files
if use_existing_file:
    pass
else:
    if clear_folder:
        for f in output_folder.list_paths_in_partition(''):
            output_folder.delete_path(f)
            logging.info(f"deleting {f}")

# get dataframe from dataset
input_data_df = input_dataset.get_dataframe()

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
      
    # get sheet names 
    if use_partition_value_for_sheetname:
        sheet_names = []
        for i in range(len(unique_data_df)):
            unique_record = unique_data_df.iloc[i, 0:-1].values
            clean_unique_record = [str(value).strip() for value in unique_record] # convert to string with no whitespaces
            clean_unique_record = ["_".join(value.split()) for value in clean_unique_record] # close any spaces between words
            sheet_name = ['_'.join(f'{c}' for c in clean_unique_record)][0]
            sheet_names.append(sheet_name)

# if columns are not partitioned
# sheet_name entered in the UI or default "Sheet1" will be used

# get file name

data_name = input_dataset_name.split(".")[-1]
current_time = datetime.datetime.now()
current_time = current_time.strftime("%m-%d-%Y-%H-%M-%S")

if not use_existing_file:   
    if include_timestamp:
        if file_name:
             excel_file_name = f"{file_name}_{current_time}.xlsx"
        else:
            excel_file_name = f"{data_name}_{current_time}.xlsx"
            
    # no timestamp used    
    else:
        if file_name:
            excel_file_name = f"{file_name}.xlsx"
        else:
            excel_file_name = f"{data_name}.xlsx"
            
# if existing file is use: existing file name is required and no timestamp field
else:
    excel_file_name = f"{existing_file}.xlsx"
    
    
# write data partitions or entire data to folder
def write_partitions():
    if use_existing_file:
        # read an existing file  
        with output_folder.get_download_stream(excel_file_name) as file:
            data = file.read() # binary data 
            stream = io.BytesIO(data)
            # save data as excel fomat into bytes string    
            writer = pd.ExcelWriter(stream, engine='openpyxl',  mode='a')
    else:
        stream = io.BytesIO() # create in-memory binary data stream 
        writer = pd.ExcelWriter(stream, engine='openpyxl')
   
    if partitioning_columns:
        i=0
        for dframe in dfs:
            dframe = dframe.applymap(str)
            if use_partition_value_for_sheetname:
                dframe.to_excel(writer, sheet_name=sheet_names[i], startrow=start_row, startcol=start_col, encoding='utf-8', index = None, header = True)
            else:
                sheet_name_1 = "Sheet" + str(i+1)
                dframe.to_excel(writer, sheet_name=sheet_name_1, startrow=start_row, startcol=start_col, encoding='utf-8', index = None, header = True)            
            i=i+1
            writer.save()

    # write entire dataframe
    else:
        if columns_to_exclude:
            df_frame = input_data_df.drop(columns_to_exclude, axis=1) 
        else:
           df_frame = input_data_df.copy()
        dframe = df_frame.applymap(str)
        if sheet_name:
            dframe.to_excel(writer, sheet_name=sheet_name, startrow=start_row, startcol=start_col, encoding='utf-8', index = None, header = True)
        else:
            dframe.to_excel(writer, sheet_name="Sheet1", startrow=start_row, startcol=start_col, encoding='utf-8', index = None, header = True)    
        writer.save()
    stream.seek(0)

    with output_folder.get_writer(excel_file_name) as writer:
        writer.write(stream.read())


write_partitions()
logging.info("Finished writing the file(s) to the folder")


