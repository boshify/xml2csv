import streamlit as st
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from io import StringIO

# Function to parse XML and convert to CSV
def xml_to_csv(xml_data):
    # Parse the XML file
    tree = ET.ElementTree(ET.fromstring(xml_data))
    root = tree.getroot()

    # Extract data into rows
    rows = []
    headers = []

    # Loop over all elements and build rows and headers dynamically
    for child in root:
        row_data = []
        for sub_child in child:
            if sub_child.tag not in headers:
                headers.append(sub_child.tag)
            row_data.append(sub_child.text)
        rows.append(row_data)

    # Create a DataFrame
    df = pd.DataFrame(rows, columns=headers)

    return df

# Streamlit app interface
st.title("XML to CSV Converter")

# URL input
url = st.text_input("Enter the URL of the XML file")

# Process the XML file
if st.button("Convert to CSV"):
    if url:
        try:
            # Fetch the XML content from the URL
            response = requests.get(url)
            if response.status_code == 200:
                xml_data = response.text

                # Convert XML to CSV
                df = xml_to_csv(xml_data)

                # Convert DataFrame to CSV
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue()

                # Provide download button
                st.download_button(label="Download CSV", data=csv_data, file_name="converted_data.csv", mime="text/csv")
            else:
                st.error("Error: Unable to fetch XML file. Please check the URL.")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.warning("Please enter a valid URL.")

