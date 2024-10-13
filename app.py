import streamlit as st
import pandas as pd
import requests
from lxml import etree
import csv
from io import StringIO

# Helper function to flatten XML elements for one entity
def flatten_element(element, parent_prefix=""):
    flat_data = {}
    
    # Process element's attributes as columns
    for attr_name, attr_value in element.attrib.items():
        key = f"{parent_prefix}{element.tag}_@{attr_name}"
        flat_data[key] = attr_value

    # Process element's text content, strip whitespace
    if element.text and element.text.strip():
        flat_data[f"{parent_prefix}{element.tag}"] = element.text.strip()

    # Recursively process child elements, ensuring they're part of the same row
    for child in element:
        child_data = flatten_element(child, parent_prefix=f"{parent_prefix}{element.tag}_")
        flat_data.update(child_data)

    return flat_data

# Function to parse XML and stream it to CSV in batches, with real-time table preview
def stream_xml_to_csv_with_preview(xml_stream, csv_file, stop_flag, root_tag):
    # Initialize progress and table preview
    progress = st.progress(0)
    table_placeholder = st.empty()
    preview_data = []  # For previewing the table
    headers = set()  # Track unique headers dynamically using a set
    batch_size = 100  # Process in smaller batches of 100 rows for real-time updates
    rows = []
    total_elements = 0

    # Use lxml's iterparse for efficient streamed parsing, target the specific root entity
    context = etree.iterparse(xml_stream, events=("end",), tag=root_tag, recover=True)

    csv_writer = csv.writer(csv_file)
    wrote_header = False  # To write header only once

    # Process each XML element
    for event, elem in context:
        if stop_flag():  # Stop if the user clicks "Stop" button
            st.warning("Process stopped by the user.")
            break

        if elem is not None and elem.tag == root_tag:
            # Flatten the element into a dictionary
            row_data = flatten_element(elem)

            # Add new headers dynamically as we discover them
            headers.update(row_data.keys())

            # Only add the row if it has at least one non-empty value
            if any(value.strip() for value in row_data.values()):
                rows.append(row_data)
                preview_data.append(row_data)  # Add to the preview data
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
            if total_elements % 100 == 0:
                progress.progress(min(total_elements / 50000, 1.0))  # Adjust based on estimated size

            # Clear memory for processed elements
            elem.clear()

            # Display the preview table after each batch
            if preview_data:
                df_preview = pd.DataFrame(preview_data)
                table_placeholder.dataframe(df_preview)

    # Write any remaining rows
    if len(rows) > 0 and not stop_flag():
        if not wrote_header:
            headers_list = sorted(headers)
            csv_writer.writerow(headers_list)
        for row in rows:
            csv_writer.writerow([row.get(header, '') for header in headers_list])

    return csv_file, pd.DataFrame(preview_data)  # Return both the CSV file and preview DataFrame

# Streamlit app interface
st.title("Flexible XML to CSV Converter with Live Table Preview")

# URL input
url = st.text_input("Enter the URL of the XML file")
root_tag = st.text_input("Enter the root tag for the entities (e.g., 'Product')")

# Button to stop the process
stop_processing = st.button("Stop Conversion")

# Function to return the stop flag status
def stop_flag():
    return stop_processing

# Process the XML file
if st.button("Start Conversion"):
    if url and root_tag:
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

                    csv_file, df_preview = stream_xml_to_csv_with_preview(response.raw, csv_file, stop_flag, root_tag)

                    # Step 2: Provide download button if process completes or is stopped
                    st.success("Conversion complete or stopped! Click the button below to download the CSV.")
                    st.download_button(label="Download CSV", data=csv_file.getvalue(), file_name="converted_data.csv", mime="text/csv")

                    # Display the final table (after stopping or completion)
                    if not df_preview.empty:
                        st.write("Final Preview:")
                        st.dataframe(df_preview)
                else:
                    st.error(f"Error: Unable to fetch XML file. HTTP Status Code: {response.status_code}")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.warning("Please enter a valid URL and root tag.")
