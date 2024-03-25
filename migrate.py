'''
ETL Tool for Migrating Customers to a New Sales Channel

This script is designed to facilitate the migration of customer data from an old or current sales channel to a new sales channel within the platform. It automates the transformation and loading of customer data, ensuring a seamless transition with minimal manual intervention.

Input (Mandatory):
1. A full XML export of customer data from the old/current sales channel (e.g., Mekonomen B2B NO). This file should contain comprehensive customer information, including attributes, addresses, user accounts, and customer segments.
2. A CSV file listing customers with their current and new IDs, along with the new company id, store id, and store name. This file is crucial for mapping existing customers to their new identifiers and store locations within the new sales channel.

Output:
1. A delta XML file containing the data of customers who are to be migrated to the new sales channel (e.g., BCP NO). This file replicates all existing customer information from the input XML, including attributes, addresses, user accounts, and customer segments.
   Additionally, each customer in the output XML will be assigned to a new customer segment, 'CG_Mekonomen', reflecting their migration to the new sales channel.

Processing Steps:
1. The script begins by generating a unique execution identifier to tag the migration process.
2. It then prompts the user to select the input XML and CSV files. If specific files are not selected, default example files are used.
3. The CSV data is loaded into a pandas DataFrame for efficient processing. This DataFrame facilitates the identification of customers to be migrated based on their current IDs.
4. The script parses the input XML file, extracting customer data that matches the IDs listed in the CSV file.
5. For each matched customer, the script updates their ID and associated store information according to the CSV file. It also adds the new 'CG_Mekonomen' segment to their profile.
6. Certain customer attributes are modified or added if necessary, such as updating company names and store details, and setting the 'MEK_CustomerOrderNumberMandatory' attribute based on CSV input.
7. The script also handles the removal of outdated attributes like 'LastOrderDate' and 'last-logged-in' to ensure the data aligns with the requirements of the new sales channel.
8. Upon completion of the data transformation, the script generates a new XML file containing the migrated customer data. This file, along with a detailed migration log in CSV format, is saved to a designated output directory.

This tool streamlines the customer migration process, ensuring data integrity and consistency across sales channels in environments.
'''

import pandas as pd
from lxml import etree
from datetime import datetime
import os
import uuid
import re

# Set execution identifier
run_id = str(uuid.uuid4())
print(f'Starting execution with ID: {run_id}')

# Helper function to get the latest files
def get_latest_files(directory, extension, count=5):
    paths = [os.path.join(directory, filename) for filename in os.listdir(directory) if filename.endswith(extension)]
    sorted_files = sorted(paths, key=os.path.getmtime, reverse=True)[:count]
    return sorted_files

# Present options and get user selection
def select_file(files, file_type):
    if not files:
        print(f"No {file_type} files found. Using default example file from example folder.")
        return None
    print(f"Select a {file_type} file by number (leave blank to use default example file from example folder):")
    for i, file in enumerate(files, 1):
        print(f"{i}. {file}")
    selection = input("Enter number: ").strip()
    return files[int(selection)-1] if selection.isdigit() and 0 < int(selection) <= len(files) else None

# Define directories to search for files
csv_directory = 'input'  # Adjust this to the directory where your CSV files are located
xml_directory = 'input'  # Adjust this to the directory where your XML files are located

# Get the latest files
latest_csv_files = get_latest_files(csv_directory, '.csv')
latest_xml_files = get_latest_files(xml_directory, '.xml')

# User selection
selected_csv_file = select_file(latest_csv_files, 'CSV')
selected_xml_file = select_file(latest_xml_files, 'XML')

# File input settings with fallback to examples
customer_include_csv = selected_csv_file if selected_csv_file else 'input/examples/Example Customer Migration List.csv'
customer_source_xml = selected_xml_file if selected_xml_file else 'input/examples/Example Full Customer Export from MekB2BNO.xml'

# Define your namespaces
ns = {
    'i': 'http://www.intershop.com/xml/ns/intershop/customer/impex/7.3',
    'dt': 'http://www.intershop.com/xml/ns/enfinity/6.5/core/impex-dt'
}

# Load the CSV file
print(f"Loading CSV file: '{customer_include_csv}")
csv_df = pd.read_csv(customer_include_csv)

# Create a set of customer IDs from the CSV for efficient lookup
customer_ids_from_csv = set(csv_df['current_customer_id'].astype(str))

# Count customer IDs from CSV file
customer_csv_count = len(customer_ids_from_csv)

# Load the XML file
print(f"Loading customer source XML file: '{customer_source_xml}'")
xml_file_path = customer_source_xml
tree = etree.parse(xml_file_path)
root = tree.getroot()

# Prepare a new XML root for the output file
new_root = etree.Element(root.tag, nsmap=root.nsmap)  # Preserves namespaces

# Get today's date in the format YYYY-MM-DD
today_date = datetime.now().strftime('%Y-%m-%dT00:00:00+00:00')

# Prepare for consistency check
customers_found_in_xml = 0

# Initialize the DataFrame for logging the process
log_columns = ['current_id', 'new_id', 'status', 'reason']
migration_log_df = pd.DataFrame(columns=log_columns)

print("Checking if customers exists in source XML...")
# Iterate through each customer ID from the CSV
for index, row in csv_df.iterrows():
    current_id = str(row['current_customer_id'])
    new_id = str(row['new_customer_id']).strip()
    # Initialize log row
    log_row = {'current_id': current_id, 'new_id': new_id, 'status': '', 'reason': ''}
    
    # Search for the customer in the XML
    customer = root.find(f'.//i:customer[@id="{current_id}"]', namespaces=ns)
    if customer is not None:
        # Customer found in XML, process as before...
        customers_found_in_xml += 1
        print(f"Customer found: '{current_id}' >> '{new_id}'")
        current_id = str(customer.get('id')).strip()

        if current_id in customer_ids_from_csv:
            row = csv_df.loc[csv_df['current_customer_id'].astype(str) == current_id].iloc[0]
            
            # Error flag for later validation
            error_flag = False
            
            new_id = row['new_customer_id']
            new_store_id = row['new_store_id']
            new_store_name = row['new_store_name']
            new_source_id = row['new_source_id']
            mandatory_ref = row['mandatory_reference']
            delivery_day = row['delivery_day']
            
            # Checks for empty fields in the CSV input
            if any(value is None for value in [new_id, new_store_id, new_store_name, new_source_id]):
                error_flag = True
                error_reason = "Missing values!"
                
            # Assuming delivery_day might be a float, string, or None
            print(f'Debug delivery_day: {delivery_day}')

            if delivery_day == "" or delivery_day is None or pd.isna(delivery_day):
                delivery_day = "-3D"
            else:
                # Convert delivery_day to string to ensure compatibility with re.match()
                delivery_day_str = str(delivery_day)
                if not re.match(r"-\b[1-9]D\b", delivery_day_str):
                    error_flag = True
                    error_reason = "Invalid delivery day format! - Eg: '-1D’"
            
            # Update the customer ID
            customer.attrib['id'] = str(new_id)
            log_row = {'current_id': current_id, 'new_id': str(new_id), 'status': '', 'reason': 'Found in source XML file'}
            
            ## Find and update the specified customer attributes
            # Flag to check if the attribute exists
            mek_customer_order_number_mandatory_exists = False
            mek_deliveryday_exists = False
            
            for attr in customer.findall('.//i:custom-attribute', ns):
                if attr.get('name') == 'MEK_Company' and attr.text == 'Mekonomen':
                    attr.text = 'Meca'
                elif attr.get('name') == 'MEK_Store_Id':
                    attr.text = str(new_store_id)
                elif attr.get('name') == 'MEK_WarehouseID':
                    attr.text = str(new_store_id)
                elif attr.get('name') == 'MEK_Store_Name':
                    attr.text = str(new_store_name)
                elif attr.get('name') == 'MEK_DataAreaID':
                    attr.text = str(new_source_id)
                elif attr.get('name') == 'MEK_SourceID':
                    attr.text = str(new_source_id)
                elif attr.get('name') == 'MEK_SystemID':
                    attr.text = "6"
                
                elif attr.get('name') == 'MEK_CustomerOrderNumberMandatory' and mandatory_ref is not None:
                    attr.text = str(mandatory_ref)
                    mek_customer_order_number_mandatory_exists = True
                    
                elif attr.get('name') == 'MEK_DefaultDeliveryday' and delivery_day is not None:
                    attr.text = str(delivery_day)
                    mek_deliveryday_exists = True
                
            
            # If the MEK_DefaultDeliveryday attribute was not found, create and append it
            if not mek_deliveryday_exists and delivery_day is not None:
                custom_attributes_element = customer.find('.//i:custom-attributes', ns)
                if custom_attributes_element is None:
                    custom_attributes_element = etree.SubElement(customer, f"{{{ns['i']}}}custom-attributes")
                mek_attr = custom_attributes_element.find('.//i:custom-attribute[@name="MEK_DefaultDeliveryday"]', ns)
                if mek_attr is not None:
                    # Update existing attribute
                    mek_attr.text = str(delivery_day)
                    mek_attr.set(f"{{{ns['dt']}}}dt", "string")  # Ensuring the dt:dt="boolean" attribute is set
                else:
                    # Add new attribute
                    new_mek_attr = etree.SubElement(custom_attributes_element, f"{{{ns['i']}}}custom-attribute", name="MEK_DefaultDeliveryday")
                    new_mek_attr.text = str(delivery_day)
                    new_mek_attr.set(f"{{{ns['dt']}}}dt", "string")  # Ensuring the dt:dt="boolean" attribute is set
                    
            # If the MEK_CustomerOrderNumberMandatory attribute was not found, create and append it
            if not mek_customer_order_number_mandatory_exists and mandatory_ref is not None and pd.notna(mandatory_ref):
                custom_attributes_element = customer.find('.//i:custom-attributes', ns)
                if custom_attributes_element is None:
                    custom_attributes_element = etree.SubElement(customer, f"{{{ns['i']}}}custom-attributes")
                mek_attr = custom_attributes_element.find('.//i:custom-attribute[@name="MEK_CustomerOrderNumberMandatory"]', ns)
                if mek_attr is not None:
                    # Update existing attribute
                    mek_attr.text = str(mandatory_ref).lower()
                    mek_attr.set(f"{{{ns['dt']}}}dt", "boolean")  # Ensuring the dt:dt="boolean" attribute is set
                else:
                    # Add new attribute
                    new_mek_attr = etree.SubElement(custom_attributes_element, f"{{{ns['i']}}}custom-attribute", name="MEK_CustomerOrderNumberMandatory")
                    new_mek_attr.text = str(mandatory_ref).lower()
                    new_mek_attr.set(f"{{{ns['dt']}}}dt", "boolean")  # Ensuring the dt:dt="boolean" attribute is set

            # Update user objects with the new customer ID
            for user in customer.findall('.//i:user', ns):
                # Update attributes and text nodes containing the old customer ID
                if user.get('business-partner-no') == current_id:
                    user.set('business-partner-no', str(new_id))
                for element in user.iter():
                    if element.text == current_id:
                        element.text = str(new_id)
                        
                # Check if the new user-group already exists
                user_groups = user.find('.//i:user-groups', ns)
                if user_groups is not None:
                    existing_ids = {ug.get('id') for ug in user_groups.findall('.//i:user-group', ns)}
                    if "CG_Mekonomen" not in existing_ids:
                        # Add the new user-group since it's not present
                        new_ug = etree.SubElement(user_groups, '{http://www.intershop.com/xml/ns/intershop/customer/impex/7.3}user-group')
                        new_ug.attrib['id'] = "CG_Mekonomen"

                # Remove 'LastOrderDate' from custom-attributes
                custom_attributes = user.find('.//i:custom-attributes', ns)
                if custom_attributes is not None:
                    for last_order_date in custom_attributes.findall('.//i:custom-attribute[@name="LastOrderDate"]', ns):
                        custom_attributes.remove(last_order_date)

                # Remove 'last-logged-in' from credentials
                credentials = user.find('.//i:credentials', ns)
                if credentials is not None:
                    last_logged_in = credentials.find('.//i:last-logged-in', ns)
                    if last_logged_in is not None:
                        credentials.remove(last_logged_in)
                
                # Update 'creation-date' with today's date
                profile = user.find('.//i:profile', ns)
                if profile is not None:
                    creation_date = profile.find('.//i:creation-date', ns)
                    if creation_date is not None:
                        creation_date.text = today_date
            
            # Append modified customer to new_root
            new_root.append(etree.fromstring(etree.tostring(customer)))
            
        if error_flag:
            log_row.update({'status': 'Not OK', 'reason': f'Invalid value! Please check CSV input file ({error_reason})'}) 
        else:
            log_row.update({'status': 'OK'})
    else:
        # Customer not found in XML
        print(f'Customer not found in source XML: {current_id}')
        log_row.update({'status': 'Not OK', 'reason': 'Not found in source XML'})
        
    # Add log row to DataFrame
    log_row_df = pd.DataFrame([log_row])  # Create a DataFrame from the single row
    migration_log_df = pd.concat([migration_log_df, log_row_df], ignore_index=True)
        
print(f'{customers_found_in_xml}/{customer_csv_count} customers found and will be included in output XML.')

# Copy schemaLocation and other attributes to make sure that it conforms to pass import validation in backoffice
schema_location = root.get('{http://www.w3.org/2001/XMLSchema-instance}schemaLocation')
new_root.set('{http://www.w3.org/2001/XMLSchema-instance}schemaLocation', schema_location)

# Copy other attributes such as 'major', 'minor', etc., if they exist
for attribute in ['major', 'minor', 'family', 'branch', 'build']:
    if root.get(attribute):
        new_root.set(attribute, root.get(attribute))

# Construct a new XML tree from the new root
new_tree = etree.ElementTree(new_root)

# Generate a UUID4 filename for output file
filename_xml = "output-" + run_id + ".xml"
filename_csv = "log-" + run_id + ".csv"

# Create the directory based on the current date inside "output" folder
folder_date = datetime.now().strftime("%Y-%m-%d")
output_directory = os.path.join("output", folder_date)
os.makedirs(output_directory, exist_ok=True)

# Write the modified XML to a new file
output_xml_file_path = os.path.join(output_directory, filename_xml)
print(f'Creating output file...')
new_tree.write(output_xml_file_path, pretty_print=True, xml_declaration=True, encoding="UTF-8")

print("Customer migration completed!")
print(f'Output file: {output_xml_file_path}')

# Export the migration log to a CSV file
log_file_path = os.path.join(output_directory, filename_csv)
migration_log_df.to_csv(log_file_path, index=False)
print(f"Migration log saved to: {log_file_path}")