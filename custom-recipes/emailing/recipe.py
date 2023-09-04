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

# get email header parameter values
sender_name = get_recipe_config()["sender_name"]
sender_email = get_recipe_config()["sender_email"]
recipient_emails = get_recipe_config()["recipient_emails"]
cc = get_recipe_config().get("cc", None)
bcc = get_recipe_config().get("bcc", None)
email_subject = get_recipe_config().get("email_subject", "")

# get file format
file_format = get_recipe_config().get('attachment_type', "CSV attachement")

# get email body
use_email_body_text = get_recipe_config().get('use_email_body_text', False)
use_email_body_text = get_recipe_config().get("use_email_body_text", "")
recipient_name_column = get_recipe_config().get("recipient_name_column", None)

# get SMTP authentication server parameter values
smtp_host = get_recipe_config()["smtp_host"]
smtp_port = get_recipe_config()["smtp_port"]
smtp_use_tls = get_recipe_config().get('smtp_use_tls', False)
smtp_use_ssl = get_recipe_config().get('smtp_use_ssl', False)
smtp_use_auth = get_recipe_config().get('smtp_use_auth', False)
smtp_user = get_recipe_config().get('smtp_use', None)
smtp_password = get_recipe_config().get('smtp_password', None)

# get data management parameter values  
partitioning_column = get_recipe_config().get('smtp_password', "")
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

# get dataframe partitions
partition_dfs = []
partition_values = input_data_df[partitioning_column].unique()
for partition in partition_values:
    partition_df = input_data_df[input_data_df[partitioning_column]==partition]
    partition_dfs.append(partition_df)

# convert dataframe to csv
def get_csv_partition(partition_df):
    # convert dataframe to csv file
    data = partition_df.to_csv(index=False)
    return data

# convert dataframe to html
def pretty_table(df_partition):
    html_table = build_table(df_partition, "blue_light")
    return html_table

# email data partition 
def send_email_tls(partition_df):
    msg = MIMEMultipart()
    msg["From"] = sender_name
    msg["To"] = recipient_emails # string
    msg["Subject"] = email_subject
    msg["CC"] = cc

    email_text = 
    file_name = f"{partition_value}.csv"

    # get data partition
    data = get_csv_partition(partition_df)
    html_table = pretty_table(partition_df)

    # Leave some space for proper displaying of the attachment
    part1 = MIMEText(email_text + '\n\n' + html_table, _subtype='html', _charset= "UTF-8")
    part2 = MIMEApplication(data)
    part2['Content-Disposition'] = f'attachment; filename="{file_name}"'

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case, the HTML message, is best and preferred.
    msg.attach(part1)
    msg.attach(part2)

    try:
        logging.info(f"Sending email to {recipient_email_list}")
        # connect to smtp server and switch connection to tls encryption
        with smtplib.SMTP(smtp_host, port=smtp_port) as smtp_client:
            smtp_client.starttls()
            # authenticate into the smtp server
            smtp_client.login(smtp_user, smtp_password)
            # send email message/attachment
            smtp_client.sendmail(from_addr=sender_email,
                                 to_addrs=recipient_email_list.split(",") + cc_list.split(",") + bc_list.split(","),
                                 msg=msg.as_string())
            # log success message
            logging.info(f"Email was successfully sent to {recipient_email_list} ")

    except Exception as e:
        logging.exception("Email sending failed")
        #logging.execption(e)

# send emails
partition_values = input_data_df[partitioning_column].unique()
i=0
for partition_df in partition_dfs:
    partition_value = partition_values[i]
    send_email_tls(partition_df)
    i = i+1










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
if write_data_to_folder:
    if include_timestamp:
        write_partitions_timestamp(input_data_df)
    else:
        write_partitions(input_data_df)

    logging.info("Finished writing CSV files to the folder")



