import streamlit as st
import pandas as pd
import requests
from lxml import etree
from io import BytesIO, StringIO
import csv
import xml.etree.ElementTree as ET

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

# Function to parse XML and preview the first element
def parse_xml_preview(xml_stream):
    preview_data = []
    headers = set()
    context = etree.iterparse(xml_stream, events=("end",), recover=True)

    # Parse the first element for preview
    root_tag = None
    raw_xml_sample = None
    for event, elem in context:
        root_tag = elem.tag
        raw_xml_sample = ET.tostring(elem, encoding='unicode')  # Store the raw XML for preview
        row_data = flatten_element(elem)
        headers.update(row_data.keys())
        preview_data.append(row_data)
        elem.clear()
        break  # Only parse the first element

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
            xml_stream = BytesIO(response.raw.read(40960))  # Use 40KB for preview

        # Step 1: Provide preview of data
        df_preview, root_tag, raw_xml_sample = parse_xml_preview(xml_stream)
        if root_tag:
            st.write(f"Detected root tag: {root_tag}")
            st.write("Raw XML Sample:")
            st.code(raw_xml_sample, language='xml')
            st.dataframe(df_preview)

            # Step 2: Mapping XML attributes to CSV columns
            st.write("Map XML attributes to CSV columns")
            mapping = {}
            for column in df_preview.columns:
                selected = st.selectbox(f"Map '{column}' to CSV column:", ["Ignore"] + list(df_preview.columns), index=0)
                if selected != "Ignore":
                    mapping[column] = selected

            # Step 3: Convert to CSV after mapping
            if st.button("Convert to CSV"):
                csv_file = StringIO()
                csv_writer = csv.writer(csv_file)
                headers = list(mapping.values())
                csv_writer.writerow(headers)

                # Parse and write all elements based on the mapping
                response = requests.get(xml_url, stream=True) if xml_url else uploaded_file
                xml_stream = BytesIO(response.raw.read()) if xml_url else BytesIO(uploaded_file.read())
                context = etree.iterparse(xml_stream, events=("end",), tag=root_tag, recover=True)
                for _, elem in context:
                    row_data = flatten_element(elem)
                    row = [row_data.get(xml_col, '') for xml_col in mapping.keys()]
                    csv_writer.writerow(row)
                    elem.clear()

                # Provide CSV download
                st.download_button(label="Download CSV", data=csv_file.getvalue(), file_name="converted_data.csv", mime="text/csv")
    except Exception as e:
        st.error(f"An error occurred: {e}")
