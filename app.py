import streamlit as st
import pandas as pd
import requests
from lxml import etree
import csv
from io import StringIO

# Function to parse XML and stream it to CSV in batches
def stream_xml_to_csv(xml_stream, csv_file):
    # Initialize progress
    progress = st.progress(0)
    status_text = st.empty()

    headers = set()  # Track unique headers dynamically
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
            row_data = {}

            # Extract each product's data
            for child in elem:
                headers.add(child.tag)
                row_data[child.tag] = child.text

            rows.append(row_data)
            total_elements += 1

            # Write batch to CSV when the batch size is reached
            if len(rows) == batch_size:
                if not wrote_header:
                    # Write the header once
                    csv_writer.writerow(headers)
                    wrote_header = True

                # Write the rows
                for row in rows:
                    csv_writer.writerow([row.get(header, '') for header in headers])

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
            csv_writer.writerow(headers)
        for row in rows:
            csv_writer.writerow([row.get(header, '') for header in headers])

    return csv_file

# Streamlit app interface
st.title("Fast XML to CSV Converter for Large Files")

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
