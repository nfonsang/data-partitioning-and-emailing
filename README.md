# Data Partitioning and Emailing Plugin
![image](https://github.com/nfonsang/data-partitioning-and-emailing/assets/45580710/c7b9a8de-dc59-436b-b5db-c84432e89fa3)

# The Goal of the Plugin
The plugin provides three recipe components for partitioning data into files, then writing the files to folders or emailing the data partitions as embedded tables or attached files. 

# How to Set-up the Plugin
After installing the plugin, configure the SMTP server presets with the user email and password associated with the SMTP server of the email sender. There are two SMTP server presets that can be set up through the settings of the plugin: the SMTP Host Server Personal Preset and SMTP Host Server Shared Preset. The SMTP Host Server Personal Preset allows the sender to authenticate into the SMTP Server through their DSS profile. The SMTP Host Server Shared Preset allows authentication into the sender SMTP Server at the instance level. 
![image](https://github.com/nfonsang/data-partitioning-and-emailing/assets/45580710/aa9a6dfd-51e9-4ba7-91c0-99527bd6f54b)

# How to Use the Plugin

## Data Partitioning Recipe
This plugin recipe component partitions data and saves the partitions as CSV or EXCEL files in a managed folder. 
![image](https://github.com/nfonsang/data-partitioning-and-emailing/assets/45580710/8d0a4bf9-313a-4d1c-a072-a95d89cf0df4)
![image](https://github.com/nfonsang/data-partitioning-and-emailing/assets/45580710/45d581cd-473a-416c-8f71-4ab59f4c3a80)
![image](https://github.com/nfonsang/data-partitioning-and-emailing/assets/45580710/1facd6c6-77cf-44f4-b16f-0e3092e83ab6)

## Emailing Data Partitions Recipe
This recipe component partitions data and emails the data partitions as CSV files or embedded HTML tables to 
recipients. Emailed data partitions can be optinally saved in a folder. 
![image](https://github.com/nfonsang/data-partitioning-and-emailing/assets/45580710/890098f1-5d95-4eec-8fb1-d0e751565318)
![image](https://github.com/nfonsang/data-partitioning-and-emailing/assets/45580710/08ce0bba-b610-4bfd-8301-d97bc96f2521)

## Write Data Partitions to Multiple Sheets in an Excel File Recipe
This recipe component partitions data and writes data partitions to multiple sheets in an Excel file in a managed folder.
![image](https://github.com/nfonsang/data-partitioning-and-emailing/assets/45580710/37612598-cc1f-4f76-99e8-4578493827c0)
![image](https://github.com/nfonsang/data-partitioning-and-emailing/assets/45580710/3c3a4f34-40a7-4f0b-bb30-76da5288b60f)

# Python Environment
This plugin requires a Python environment to work. The Python environment can be created when the plugin is being installed, and a specific version of Python could be selected among various versions of Python, including PYTHON39, PYTHON10, and PYTHON11.

# License
This plugin is distributed under the [Apache License version 2.0.](https://github.com/nfonsang/data-partitioning-and-emailing/blob/main/LICENSE)
