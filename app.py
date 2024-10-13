import streamlit as st
import pandas as pd
import requests
from lxml import etree
import csv
from io import StringIO

# Helper function to recursively flatten XML elements
def flatten_element(element, parent_prefix=""):
    flat_data = {}
    
    # Process element's attributes as columns
    for attr_name, attr_value in element.attrib.items():
        key = f"{parent_prefix}{element.tag}_@{attr_name}"
        flat_data[key] = attr_value

    # Process element's text content
    if element.text and element.text.strip():
        flat_data[f"{parent_prefix}{element.tag}"] = element.text.strip()

    # Recursively process child elements
    for child in element:
        child_data = flatten_element(child, parent_prefix=f"{parent_prefix}{element.tag}_")
        flat_data.update(child_data)

    return flat_data

# Function to parse XML and stream it to CSV in batches
def stream_xml_to_csv(xml_stream, csv_file):
    # Initialize progress
    progress = st.progress(0)
    status_text = st.empty()

    headers = set()  # Track unique headers dynamically using a set
    batch_size = 1000  # Process in batches of 1000 rows
    rows = []
    total_elements = 0

    # Use lxml's iterparse for efficient streamed parsing
    context = etree.iterparse(xml_stream, events=("end",), recover=True)

    csv_writer = csv.writer(csv_file)
    wrote_header = False  # To write header only once

    # Process each XML element
    for event, elem in context:
        if elem is not None and elem.tag is not None:
            # Flatten the element into a dictionary
            row_data = flatten_element(elem)

            # Add new headers dynamically as we discover them
            headers.update(row_data.keys())  # Correctly using `update()` on a set

            rows.append(row_data)
            total_elements += 1

            # Write batch to CSV when the batch size is reached
            if len(rows) == batch_size:
                if not wrote_header:
                    # Write the header once
                    headers_list = sorted(headers)  # Sorting headers to maintain consistent column order
                    csv_writer.writerow(headers_list)
                    wrote_header = True

                # Write the rows
                for row in rows:
                    # Ensure all columns are filled, even if some data is missing
                    csv_writer.writerow([row.get(header, '') for header in headers_list])

                # Clear batch
                rows = []

            # Update progress bar every 1000 rows
            if total_elements % 1000 == 0:
                progress.progress(min(total_elements / 50000, 1.0))  # Adjust based on estimated size

            # Clear memory for processed elements
            elem.clear()

    # Write any remaining rows
    if len(rows) > 0:
        if not wrote_header:
            headers_list = sorted(headers)
            csv_writer.writerow(headers_list)
        for row in rows:
            csv_writer.writerow([row.get(header, '') for header in headers_list])

    return csv_file

# Streamlit app interface
st.title("Flexible XML to CSV Converter for Large Files")

# URL input
url = st.text_input("Enter the URL of the XML file")

# Process the XML file
if st.button("Convert to CSV"):
    if url:
        try:
            # Step 1: Stream the XML file using requests with stream=True
            st.write("Fetching XML file... This can take a while for very large files.")

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
            }

            with requests.get(url, headers=headers, stream=True) as response:
                if response.status_code == 200:
                    # Stream the XML data and save CSV directly to a file
                    csv_file = StringIO()  # In-memory file to write the CSV data
                    st.write("Parsing and converting XML to CSV...")

                    csv_file = stream_xml_to_csv(response.raw, csv_file)

                    # Step 2: Provide download button
                    st.success("Conversion complete! Click the button below to download the CSV.")
                    st.download_button(label="Download CSV", data=csv_file.getvalue(), file_name="converted_data.csv", mime="text/csv")
                else:
                    st.error(f"Error: Unable to fetch XML file. HTTP Status Code: {response.status_code}")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.warning("Please enter a valid URL.")
