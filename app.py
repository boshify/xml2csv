import streamlit as st
import pandas as pd
import requests
from lxml import etree
from io import StringIO

# Function to parse XML and convert to CSV using lxml and streaming
def stream_xml_to_csv(xml_stream):
    # Initialize progress
    progress = st.progress(0)
    status_text = st.empty()

    # Initialize headers and rows for the CSV
    headers = []
    rows = []
    total_elements = 0

    # Use lxml's iterparse for efficient streamed parsing
    context = etree.iterparse(xml_stream, events=("end",), recover=True)

    # Process each XML element
    for event, elem in context:
        # Ensure the element is valid and has a tag
        if elem is not None and elem.tag is not None:
            if total_elements == 0:  # First time, get root tag
                context.root = elem

            # Process valid XML elements
            row_data = []

            # Extract each product's data
            for child in elem:
                if child.tag not in headers:
                    headers.append(child.tag)
                row_data.append(child.text)

            rows.append(row_data)
            total_elements += 1

            # Update progress for large files
            if total_elements % 100 == 0:
                progress.progress(min(total_elements / 10000, 1.0))  # Arbitrary upper limit for progress bar

            # Clear memory for processed elements
            elem.clear()

    # Create DataFrame from parsed data
    df = pd.DataFrame(rows, columns=headers)

    return df

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
                    # Stream the XML data
                    xml_stream = response.raw
                    st.write("Parsing and converting XML to CSV...")
                    
                    # Stream and parse the XML data
                    df = stream_xml_to_csv(xml_stream)

                    # Step 2: Convert DataFrame to CSV
                    csv_buffer = StringIO()
                    df.to_csv(csv_buffer, index=False)
                    csv_data = csv_buffer.getvalue()

                    # Step 3: Provide download button
                    st.success("Conversion complete! Click the button below to download the CSV.")
                    st.download_button(label="Download CSV", data=csv_data, file_name="converted_data.csv", mime="text/csv")
                else:
                    st.error(f"Error: Unable to fetch XML file. HTTP Status Code: {response.status_code}")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.warning("Please enter a valid URL.")
