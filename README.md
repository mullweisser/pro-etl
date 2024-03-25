# ETL Customer Migration Tool for ICM B2C/B2X
This Python script is designed to automate the migration of customer data from an old or current sales channel to a new one within the ICM B2C/B2X platform.<br /><br />It handles the extraction, transformation, and loading (ETL) of customer data, minimizing manual intervention and ensuring a smooth transition.

#### Primarily designed for internal use, do not expect it to work for you without modifications. Code is provided <i>"as is"</i>.

## Features
- **Automated Customer Migration**: Seamlessly migrate customer data to a new sales channel.
- **Data Transformation**: Update customer IDs, store information, and ensure data integrity.
- **Minimal Manual Intervention**: The process is automated to reduce manual tasks.

## Input Requirements
1. **XML Export of Customer Data**: A full XML export from the old/current sales channel.
2. **CSV File for Customer Mapping**: A CSV file listing customers with their mapping data.

    ### Description of input CSV fields
    | Column Name         | Description                                                           | Mandatory | Example |
    |---------------------|-----------------------------------------------------------------------|-----------|---------|
    | current_customer_id | Current Customer ID                                                   | Yes       | 123456  |
    | new_customer_id     | New Customer ID                                                       | Yes       | 654321  |
    | new_source_id       | Company ID of new store                                               | Yes       | 15      |
    | new_store_id        | New store ID                                                          | Yes       | SP1     |
    | new_store_name      | New store name                                                        | Yes       | Store B |
    | mandatory_reference | If customer order number is mandatory when placing order in catalogue | No        | true    |
    | delivery_day        | Lead time calculation. If empty then default value (-3D) is applied   | No        | -2D     |


## Output
- **Delta XML File**: Contains the migrated customer data with updates as per the new sales channel's requirements.
- **Migration Log**: A detailed CSV log of the migration process.

## Pre-requisites
- Python 3.x
- Pandas library
- LXML library

## Installation
1. Ensure Python 3.x is installed on your machine.
2. Install required Python libraries:
   ```sh
   pip install requirements.txt
   ```

## How to Run
1. Clone or download this script to your local machine.
2. Place your input XML and CSV files in the designated `input` directory.<br /><sub>Adjust the `csv_directory` and `xml_directory` variables in the script if your files are located elsewhere.</sub>
3. Open a terminal or command prompt.
4. Navigate to the directory where the script is located.
5. Run the script with Python:
   ```sh
   python migrate.py
   ```
6. Follow the on-screen prompts to select the input files or use the default example files.
7. The script will process the data and output the migrated customer data in a new XML file and a detailed log in CSV format in the `output` directory.
