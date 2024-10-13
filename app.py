import streamlit as st
import pandas as pd
import requests
from lxml import etree
from io import StringIO

# Function to parse XML and convert to CSV using lxml for faster parsing
def xml_to_csv(xml_data):
    # Initialize progress
    progress = st.progress(0)
    status_text = st.empty()

    # Parse the XML file efficiently with lxml iterparse
    context = etree.iterparse(StringIO(xml_data), events=("end",), recover=True)

    headers = []
    rows = []
    total_elements = 0  # We can't estimate the total upfront

    # Loop through parsed XML events
    for _, elem in context:
        if elem.tag == context.root.tag:
            row_data = []

            for child in elem:
                if child.tag not in headers:
                    headers.append(child.tag)
                row_data.append(child.text)

            rows.append(row_data)
            total_elements += 1

            # Update progress every 100 elements to avoid too frequent updates
            if total_elements % 100 == 0:
                progress.progress(min(total_elements / 10000, 1.0))  # Arbitrary upper limit for progress bar

            # Clear the element to reduce memory usage
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
            # Step 1: Fetch XML file with detailed headers to simulate real browser requests
            st.write("Fetching XML file...")

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

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                xml_data = response.text

                # Step 2: Parse and convert to CSV with optimizations
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
                st.error(f"Error: Unable to fetch XML file. HTTP Status Code: {response.status_code}")
        except Exception as e:
            st.error(f"An error occurred: {e}")
    else:
        st.warning("Please enter a valid URL.")
