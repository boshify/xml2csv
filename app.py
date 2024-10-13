import streamlit as st
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from io import StringIO

# Function to parse XML and convert to CSV
def xml_to_csv(xml_data):
    # Initialize progress
    progress = st.progress(0)
    status_text = st.empty()

    # Parse the XML file efficiently with iterparse
    context = ET.iterparse(StringIO(xml_data), events=("start", "end"))
    _, root = next(context)  # Get the root element

    headers = []
    rows = []
    total_elements = len(list(root))  # Estimate total elements for progress tracking
    processed_elements = 0

    # Update status
    status_text.text("Parsing XML and converting to CSV...")

    # Process XML elements
    for event, elem in context:
        if event == "end" and elem.tag == root.tag:
            # Gather row data
            row_data = []
            for child in elem:
                if child.tag not in headers:
                    headers.append(child.tag)
                row_data.append(child.text)

            rows.append(row_data)
            processed_elements += 1

            # Update progress bar
            progress.progress(processed_elements / total_elements)

            # Clear element to keep memory usage low
            elem.clear()

    # Create a DataFrame from the parsed data
    df = pd.DataFrame(rows, columns=headers)

    return df

# Streamlit app interface
st.title("Optimized XML to CSV Converter")

# URL input
url = st.text_input("Enter the URL of the XML file")

# Process the XML file
if st.button("Convert to CSV"):
    if url:
        try:
            # Step 1: Fetch XML file
            st.write("Fetching XML file...")
            response = requests.get(url)
            if response.status_code == 200:
                xml_data = response.text

                # Step 2: Parse and convert to CSV
                st.write("Converting XML to CSV... This might take a while for large files.")
                df = xml_to_csv(xml_data)

                # Step 3: Convert DataFrame to CSV
                csv_buffer = StringIO()
                df.to_csv(csv_buffer, index=False)
                csv_data = csv_buffer.getvalue()

                # Step 4: Provide download button
                st.success("Conversion complete! Click the button below to download the CSV.")
                st.download_button(label="Download CSV", data=csv_data, file_name="converted_data.csv", mime="text/csv")
            else:
                st.error("Error: Unable to fetch XML file. Please check the URL.")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.warning("Please enter a valid URL.")
