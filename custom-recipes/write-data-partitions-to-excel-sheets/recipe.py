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

partitioning_columns = get_recipe_config().get("partitioning_column", None)
if partitioning_columns==None:
    sheet_name = get_recipe_config().get("sheet_name", "Sheet1")
use_partition_value_for_sheetname = get_recipe_config().get('use_partition_value_for_sheetname', None)
columns_to_exclude = get_recipe_config().get('columns_to_exclude', "")
file_name = get_recipe_config().get('file_name', None)
start_col = get_recipe_config().get('start_col', 0)
start_row = get_recipe_config().get('start_row', 0)
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

        # get sheet names if data is partitioned
        sheet_names = []
        for item in clean_unique_values:
            item=list(item)
            string_name = '_'.join(f'{c}' for c in item)
            sheet_names.append(string_name)
        
        if use_partition_value_for_sheetname:
            clean_sheet_names = [item.split() for item in sheet_names]
            clean_sheet_names = ['_'.join(f'{c}' for c in item) for item in clean_sheet_names]

        # get dataframe partitions and file names 
        dfs =[]
        final_sheet_names = []
            
        for i in range(len(queries)):
            df_part = input_data_df.query(queries[i])
            if columns_to_exclude:
                columns = [item.strip() for item in columns_to_exclude.split(",")]
                df_part = df_part.drop(columns, axis=1) 
            if len(df_part)>0:
                dfs.append(df_part)
                sheet_name_1 = clean_sheet_names[i]
                final_sheet_names.append(sheet_name_1)
 

# if columns are not partitioned
# sheet_name entered in the UI or default "Sheet1" will be used

# get file name
if file_name:
    excel_name = f"{file_name}.xlsx"
else:
    data_name = input_dataset_name.split(".")[-1]
    excel_name = f"{data_name}.xlsx"

folder_info = output_folder.get_info()  

# write data partitions or entire data to folder
def write_partitions():
    path = os.path.join(folder_info['path'], excel_name)
    writer = pd.ExcelWriter(path)
    if partitioning_columns:   
        i=0
        for dframe in dfs:
            dframe =dframe.applymap(str)
            if use_partition_value_for_sheetname:
                with io.BytesIO() as buf:
                    df.to_excel(buf, sheet_name=final_sheet_names[i], startrow=start_row, startcol=start_col, encoding='utf-8', index = None, header = True)
            output_folder.upload_stream(excel_name, buf.getvalue())
            
            #dframe.to_excel(writer, sheet_name=final_sheet_names[i], startrow=start_row, startcol=start_col, encoding='utf-8', index = None, header = True)
            else:
                sheet_name = sheet_name + str(i)
                dframe.to_excel(writer, sheet_name=sheet_name, startrow=start_row, startcol=start_col, encoding='utf-8', index = None, header = True)            
            i=i+1
        writer.save()


            # output_folder.upload_stream(file_name, buf.getvalue())
 
    # write entire dataframe
    else:
        dframe =input_data_df.applymap(str)
        dframe.to_excel(writer, sheet_name=sheet_name, startrow=start_row, startcol=start_col, encoding='utf-8', index = None, header = True)
        writer.save()


# write partitions or entire data to folder with time stamps included
def write_partitions_timestamp():
    # get current timestamp
    current_time = datetime.datetime.now()
    current_time = current_time.strftime("%m-%d-%Y-%H-%M-%S")
    excel_name = f"{excel_name}_{current_time}.xlsx"

    path = os.path.join(folder_info['path'], excel_name)
    writer = pd.ExcelWriter(path)
    
    if partitioning_columns:
        i=0
        for dframe in dfs:
            dframe =dframe.applymap(str)
            if use_partition_value_for_sheetname:
                dframe.to_excel(writer, sheet_name=final_sheet_names[i], startrow=start_row, startcol=start_col, encoding='utf-8', index = None, header = True)
            else:
                sheet_name = sheet_name + str(i)
                dframe.to_excel(writer, sheet_name=sheet_name, startrow=start_row, startcol=start_col, encoding='utf-8', index = None, header = True)            
            i=i+1
        writer.save()


    # write entire dataframe
    else:
        dframe =input_data_df.applymap(str)
        dframe.to_excel(writer, sheet_name=sheet_name, startrow=start_row, startcol=start_col, encoding='utf-8', index = None, header = True)
        writer.save()

# function calls to write data
if include_timestamp:
    write_partitions_timestamp()
else:
    write_partitions()
    
logging.info("Finished writing files to the folder")


