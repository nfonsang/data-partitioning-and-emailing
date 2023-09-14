import dataiku
from dataiku import pandasutils as pdu
import pandas as pd
import datetime
import logging

# import packages for emailing
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from pretty_html_table import build_table

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

# get email header parameter values
sender_name = get_recipe_config()["sender_name"]
sender_email = get_recipe_config()["sender_email"]
recipient_emails = get_recipe_config().get("recipient_emails", "")
recipient_email_column = get_recipe_config().get("recipient_email_column", None)
use_recipient_email_column = get_recipe_config().get("use_recipient_email_column", None)
cc = get_recipe_config().get("cc", "")
bc = get_recipe_config().get("bc", "")
email_subject = get_recipe_config().get("email_subject", "")

# get file format
file_format = get_recipe_config().get('file_format', "csv")

# get email body
use_email_body = get_recipe_config().get('use_email_body', False)
email_body_text = get_recipe_config().get("email_body_text", "")


# Get SMTP Server authentication type
authentication_type = get_recipe_config().get("authentication_type", "shared-preset")

# if credentials are per user, get personal credentials and parameters 
if authentication_type=="personal_preset":
    personal_preset = get_recipe_config().get("smtp_personal_connection", {})
    smtp_host = personal_preset['smtp_host']
    smtp_port = personal_preset['smtp_port']
    smtp_use_tls = personal_preset['smtp_use_tls']
    smtp_use_ssl = personal_preset['smtp_use_ssl']
    smtp_use_auth = personal_preset['smtp_use_auth'] 
    smtp_user = personal_preset['smtp_personal_basic']["user"]
    smtp_password = personal_preset['smtp_personal_basic']["password"]

# if credentials are shared, get shared credential and parameters 
if authentication_type=="shared_preset":
    shared_preset = get_recipe_config().get("smtp_shared_connection", {})
    smtp_host = shared_preset['smtp_host']
    smtp_port = shared_preset['smtp_port']
    smtp_use_tls = shared_preset['smtp_use_tls']
    smtp_use_ssl = shared_preset['smtp_use_ssl']
    smtp_use_auth = shared_preset['smtp_use_auth'] 
    smtp_user = shared_preset['smtp_user']
    smtp_password = shared_preset['smtp_password']


# get data management parameter values  
partitioning_column = get_recipe_config().get('partitioning_column', "")
columns_to_exclude = get_recipe_config().get('columns_to_exclude', "")
write_data_to_folder = get_recipe_config().get('write_data_to_folder', False)
include_timestamp = get_recipe_config().get('include_timestamp', False)
clear_folder = get_recipe_config().get('clear_folder', False)


# clear folder before partitioning the datasets into CSV files)
if clear_folder:
    logging.info("clearing folder")
    output_folder.clear()

# get dataframe from dataset
input_data_df = input_dataset.get_dataframe()


# get partition dataframe and partition values from dataset
input_data_df = input_dataset.get_dataframe()
if partitioning_column:
    partition_values = input_data_df[partitioning_column].unique()
    recipient_emails_for_partitions = []
    partition_dfs = []
    for partition in partition_values:
        partition_df = input_data_df[input_data_df[partitioning_column]==partition]
        # get recipient email address(es) before dropping columns
        if use_recipient_email_column:
            rec_emails_in_partition = partition_df[recipient_email_column].unique().tolist()
            recipient_emails_for_partitions.append(rec_emails_in_partition)            
        if columns_to_exclude:
            columns = [item.strip() for item in columns_to_exclude.split(",")]
            partition_df = partition_df.drop(columns, axis=1)
        partition_dfs.append(partition_df)
                 
else:
    # for the entire dataset
    if use_recipient_email_column:
        recipient_emails_for_partitions = input_data_df[recipient_email_column].unique().tolist()
    if columns_to_exclude:
        columns = [item.strip() for item in columns_to_exclude.split(",")]
        input_data_df = input_data_df.drop(columns, axis=1)

# convert dataframe to csv
def get_csv_partition(partition_df):
    # convert dataframe to csv file
    data = partition_df.to_csv(index=False)
    return data

# convert dataframe to html
def pretty_table(df_partition):
    html_table = build_table(df_partition, "blue_light")
    return html_table

# define email data partition function 
def send_email(partition_df, partition):
    msg = MIMEMultipart()
    msg["From"] = sender_name
    if use_recipient_email_column:
        msg["To"] = rec_emails # string
    else:
        msg["To"] = recipient_emails
    msg["Subject"] = email_subject.format(partition=partition)
    msg["CC"] = cc
    file_name = f"{partition}.csv"
    
    # get data to be emailed 
    data = get_csv_partition(partition_df)
    
    # create html table to be embedded
    html_table = pretty_table(partition_df)
    
    # create email body
    if file_format=="csv":
        email_text = email_body_text.format(partition=partition, table="")
        part2 = MIMEText("<pre>" + "<div style='font-family: Cambria'>" + email_text + "</div>" + "</pre>", _subtype='html', _charset= "UTF-8")
        part1 = MIMEApplication(data)
        part1['Content-Disposition'] = f'attachment; filename="{file_name}"'
        msg.attach(part1)
        msg.attach(part2)

    if file_format=="html":
        email_text = email_body_text.format(partition=partition, table=html_table)
        part2 = MIMEText("<pre>" + "<div style='font-family: Cambria'>" + email_text + "</div>" + "</pre>", _subtype='html', _charset= "UTF-8")
        msg.attach(part2) 
    if file_format=="csv_html":
        email_text = email_body_text.format(partition=partition, table=html_table)
        part1 = MIMEApplication(data)
        part1['Content-Disposition'] = f'attachment; filename="{file_name}"'
        part2 = MIMEText("<pre>" + "<div style='font-family: Cambria'>" + email_text + "</div>" + "</pre>", _subtype='html', _charset= "UTF-8")
        msg.attach(part1)
        msg.attach(part2)
  
    try:
        if smtp_use_tls:
            # connect to smtp server and switch connection to tls encryption
            smtp_client = smtplib.SMTP(smtp_host, port=smtp_port)
            smtp_client.starttls()
        elif smtp_use_ssl:
            # connect to smtp server and switch connection to ssl encryption
            smtp_client = smtplib.SMTP_SSL(smtp_host, port=smtp_port)
        if smtp_use_auth:
            smtp_client.login(smtp_user, str(smtp_password))
            
        if (not smtp_use_tls) and (not smtp_use_tls) and (not smtp_use_auth): 
            smtp_client = smtplib.SMTP(smtp_host, port=smtp_port)
            
        # send email message/attachment
        smtp_client.sendmail(from_addr=sender_email,
                                 to_addrs=recipient_emails.split(",") + cc.split(",") + bc.split(","),
                                 msg=msg.as_string())
        # log success message
        if use_recipient_email_column:
            logging.info(f"Email with {partition} data was successfully sent to {rec_emails}")     
        else:
            logging.info(f"Email with {partition} data was successfully sent to {recipient_emails}")

    except Exception as e:
        logging.exception("Email sending failed")
        logging.exception(e)
    smtp_client.quit()

# send emails   
i=0
if partitioning_column:
    # email data partitions
    for partition_df in partition_dfs:
        partition = partition_values[i]
        if use_recipient_email_column:
            rec_emails = recipient_emails_for_partitions[i]
            rec_emails = ",".join(rec_emails)
        send_email(partition_df, partition)
        i = i+1
else:
    # email entire data
    partition = input_dataset_name.split(".")[-1]
    if use_recipient_email_column:
        rec_emails = recipient_emails_for_partitions # all emails
        rec_emails = ",".join(rec_emails)
    send_email(input_data_df, partition)


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
