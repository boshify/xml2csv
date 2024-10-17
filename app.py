import streamlit as st
import pandas as pd
import requests
from lxml import etree
from io import BytesIO, StringIO
import csv
import xml.etree.ElementTree as ET

# Set testing mode to False to process the entire XML file
testing_mode = False

# Helper function to flatten XML elements for one entity
def flatten_element(element, parent_prefix=""):
    flat_data = {}
    
    # Process element's attributes as columns
    for attr_name, attr_value in element.attrib.items():
        key = f"{parent_prefix}{element.tag}_@{attr_name}"
        flat_data[key] = attr_value

    # Process element's text content, strip whitespace
    flat_data[f"{parent_prefix}{element.tag}"] = element.text.strip() if element.text else ""

    # Recursively process child elements, ensuring they're part of the same row
    for child in element:
        child_data = flatten_element(child, parent_prefix=f"{parent_prefix}{element.tag}_")
        flat_data.update(child_data)

    return flat_data

# Function to parse XML and preview the first complex element
def parse_xml_preview(xml_stream):
    preview_data = []
    headers = set()
    context = etree.iterparse(xml_stream, events=("end",), recover=True)

    # Parse the first element with children for preview
    root_tag = None
    raw_xml_sample = None
    for event, elem in context:
        if len(elem) > 0 or elem.attrib:  # Ensure we get a complex element with attributes or children
            root_tag = elem.tag
            raw_xml_sample = ET.tostring(elem, encoding='unicode')  # Store the raw XML for preview
            row_data = flatten_element(elem)
            headers.update(row_data.keys())
            preview_data.append(row_data)
            elem.clear()
            break  # Only parse the first complex element

    # Create DataFrame for preview
    df_preview = pd.DataFrame(preview_data, columns=sorted(headers))
    return df_preview, root_tag, raw_xml_sample

# Streamlit app interface
st.title("XML to CSV Converter with Attribute Mapping")

# File uploader or URL input
uploaded_file = st.file_uploader("Upload an XML file", type="xml")
xml_url = st.text_input("Or enter the URL of the XML file")

if uploaded_file is not None or xml_url:
    try:
        if uploaded_file is not None:
            # Load XML from uploaded file
            st.write("Previewing XML data...")
            xml_stream = BytesIO(uploaded_file.read())
        elif xml_url:
            # Load XML from URL
            st.write("Fetching XML from URL...")
            response = requests.get(xml_url, stream=True)
            response.raise_for_status()
            xml_stream = BytesIO()

            # Read the XML stream in chunks until the first complex element is found
            for chunk in response.iter_content(chunk_size=4096):
                xml_stream.write(chunk)
                try:
                    xml_stream.seek(0)
                    df_preview, root_tag, raw_xml_sample = parse_xml_preview(xml_stream)
                    if root_tag:
                        break
                except ET.ParseError:
                    # Continue reading more chunks if the XML is incomplete
                    xml_stream.seek(0, 2)  # Move to the end of the stream to append more data
                    continue

        # Step 1: Provide preview of data
        if root_tag:
            st.write(f"Detected root tag: {root_tag}")
            st.write("Raw XML Sample:")
            st.code(raw_xml_sample, language='xml')
            st.dataframe(df_preview)

            # Step 2: Mapping XML attributes to CSV columns
            st.write("Map XML attributes to CSV columns")
            mapping = {}
            for column in df_preview.columns:
                selected = st.selectbox(f"Map '{column}' to CSV column:", [column, "Ignore"] + list(df_preview.columns), index=0)
                if selected != "Ignore":
                    mapping[column] = selected

            # Step 3: Convert to CSV after mapping
            if st.button("Convert to CSV"):
                csv_file = StringIO()
                csv_writer = csv.writer(csv_file)
                headers = list(mapping.values())
                csv_writer.writerow(headers)

                # Create an empty DataFrame to display ongoing results
                results_df = pd.DataFrame(columns=headers)
                results_table = st.empty()

                # Parse and write elements based on the mapping, element by element
                if uploaded_file is not None:
                    xml_stream = BytesIO(uploaded_file.read())
                elif xml_url:
                    xml_stream = BytesIO(response.content)  # Use the already downloaded content for conversion

                context = etree.iterparse(xml_stream, events=("end",), tag=root_tag, recover=True)

                # Process elements iteratively, one at a time
                for idx, (_, elem) in enumerate(context):
                    if len(elem) > 0 or elem.attrib:  # Process only complex elements
                        row_data = flatten_element(elem)
                        row = [row_data.get(xml_col, '') for xml_col in mapping.keys()]
                        csv_writer.writerow(row)

                        # Update the DataFrame and UI table with new row data
                        results_df.loc[idx] = row
                        results_table.dataframe(results_df)

                        elem.clear()

                # Provide CSV download
                st.download_button(label="Download CSV", data=csv_file.getvalue(), file_name="converted_data.csv", mime="text/csv")
    except Exception as e:
        st.error(f"An error occurred: {e}")
