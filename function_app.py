import azure.functions as func
import logging
import os
import requests
import pyodbc
import time
import re

app = func.FunctionApp()

# Environment variables for OCR and SQL connection
OCR_ENDPOINT = os.environ.get("OCR_ENDPOINT")
OCR_KEY = os.environ.get("OCR_KEY")
DB_CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING")

@app.blob_trigger(arg_name="myblob", path="uploaded-files/{name}",
                  connection="syncrowinassetstorage_STORAGE")
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Processing blob: {myblob.name}, Size: {myblob.length} bytes")

    try:
        # Read blob content
        blob_content = myblob.read()

        # Process the blob content with OCR
        ocr_result = process_ocr(blob_content)  # Send to OCR service
        extracted_text = extract_text(ocr_result)  # Extract text from OCR result

        # Extract fields from the OCR content
        parsed_data_from_text = parse_extracted_text(extracted_text)

        # Extract fields from the file name
        parsed_data_from_filename = extract_from_filename(myblob.name)

        # Combine both data sources
        parsed_data = {**parsed_data_from_text, **parsed_data_from_filename}

        # Save the extracted text and parsed fields to SQL database
        save_to_db(myblob.name, extracted_text, parsed_data)
    except Exception as e:
        logging.error(f"Failed to process blob {myblob.name}: {str(e)}")


def process_ocr(blob_content):
    """Sends the blob content to Azure OCR and returns the result."""
    headers = {
        'Ocp-Apim-Subscription-Key': OCR_KEY,
        'Content-Type': 'application/octet-stream'
    }

    # Initial OCR API call to submit the image for processing
    response = requests.post(f"{OCR_ENDPOINT}/vision/v3.2/read/analyze", headers=headers, data=blob_content)

    if response.status_code != 202:
        logging.error(f"OCR API call failed: {response.text}")
        raise Exception(f"OCR API call failed: {response.text}")

    # Get the operation location from the response headers
    operation_location = response.headers.get('Operation-Location')
    if not operation_location:
        raise Exception("Operation-Location header is missing from OCR API response.")

    logging.info("OCR processing started. Polling for results...")

    # Poll the OCR API until the processing is complete
    poll_attempts = 0
    max_poll_attempts = 10
    while poll_attempts < max_poll_attempts:
        time.sleep(3)  # Wait for 3 seconds before polling again
        poll_response = requests.get(operation_location, headers={'Ocp-Apim-Subscription-Key': OCR_KEY})

        if poll_response.status_code == 200:
            poll_result = poll_response.json()
            status = poll_result.get("status")
            logging.info(f"OCR status: {status}")

            # Check if the status indicates the process is complete
            if status == "succeeded":
                return poll_result
            elif status == "failed":
                raise Exception("OCR processing failed.")

        poll_attempts += 1
        logging.info(f"Polling attempt {poll_attempts}/{max_poll_attempts}")

    raise Exception("OCR processing took too long or did not complete.")


def extract_text(ocr_result):
    """Extracts text from the OCR response."""
    try:
        lines = [line["text"] for line in ocr_result["analyzeResult"]["readResults"][0]["lines"]]
        return "\n".join(lines)
    except KeyError as e:
        logging.error(f"Error extracting text from OCR result: {str(e)}")
        raise Exception(f"Failed to extract text from OCR result: {str(e)}")


def extract_from_filename(filename):
    """Extracts asset type and document type from the file name."""
    parsed_data = {
        "AssetType": None,
        "DocumentType": None
    }

    # Example: parsing patterns from filename (assuming a structured file name format)
    # Example filename: CP-F-RASS-C11R14_Manual.pdf

    asset_type_match = re.search(r"CP-F-\w+", filename)
    document_type_match = re.search(r"(Manual|Circuit diagrams|Maintenance)", filename, re.IGNORECASE)

    if asset_type_match:
        parsed_data["AssetType"] = asset_type_match.group(0)

    if document_type_match:
        parsed_data["DocumentType"] = document_type_match.group(1)

    return parsed_data


def parse_extracted_text(extracted_text):
    """Parses the extracted text to extract asset type, manufacturer, model number, serial number, etc."""
    parsed_data = {
        "AssetType": None,
        "Manufacturer": None,
        "ModelNumber": None,
        "SerialNumber": None,
        "DocumentType": None
    }

    manufacturer_patterns = ["FESTO", "Mitsubishi", "Siemens", "ABB"]  # Add more manufacturers as needed
    document_type_patterns = ["Manual", "Circuit diagrams", "Maintenance Manual"]  # Common document types
    asset_type_patterns = ["Robot", "Conveyor", "Assembly Cell", "Motor", "Switch", "Grundmodul"]  # Common asset types

    # Use separate patterns for model number and serial number
    model_number_match = re.search(r"\b(CP-F-\w+)\b", extracted_text)  # More specific to CP-F models
    serial_number_match = re.search(r"\b(S\-Nr\.\d+|SN\d+)\b", extracted_text)

    # Detect manufacturer
    for manufacturer in manufacturer_patterns:
        if manufacturer in extracted_text:
            parsed_data["Manufacturer"] = manufacturer
            break

    # Detect document type
    for doc_type in document_type_patterns:
        if re.search(rf"\b{doc_type}\b", extracted_text, re.IGNORECASE):
            parsed_data["DocumentType"] = doc_type
            break

    # Detect asset type
    for asset_type in asset_type_patterns:
        if re.search(rf"\b{asset_type}\b", extracted_text, re.IGNORECASE):
            parsed_data["AssetType"] = asset_type
            break

    # Extract model number
    if model_number_match:
        parsed_data["ModelNumber"] = model_number_match.group(1)

    # Extract serial number
    if serial_number_match:
        parsed_data["SerialNumber"] = serial_number_match.group(1)

    # Ensure AssetType and ModelNumber are not the same
    if parsed_data["ModelNumber"] == parsed_data["AssetType"]:
        parsed_data["ModelNumber"] = None  # Set ModelNumber to None if they are identical

    return parsed_data

def save_to_db(blob_name, extracted_text, parsed_data):
    """Saves the extracted text and metadata to Azure SQL Database using ODBC connection."""
    try:
        # ODBC Connection string with the necessary driver and encryption settings
        connection_string = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:syncrowin-sql-server.database.windows.net,1433;Database=SyncrowinAssetDB;Uid=syncrowin-db-admin;Pwd=Ch5forsyn;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        
        # Establish connection to the Azure SQL Database
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        
        # Insert blob name, extracted text, and parsed fields into the SQL table
        cursor.execute("""
            INSERT INTO AssetData (FileName, ExtractedText, AssetType, Manufacturer, ModelNumber, SerialNumber, DocumentType) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (blob_name, extracted_text, parsed_data["AssetType"], parsed_data["Manufacturer"], 
                  parsed_data["ModelNumber"], parsed_data["SerialNumber"], parsed_data["DocumentType"]))
        
        # Commit the transaction
        connection.commit()

        # Close cursor and connection
        cursor.close()
        connection.close()

        logging.info(f"Saved data for {blob_name} to database successfully.")
    except Exception as e:
        logging.error(f"Error saving data to SQL Database: {str(e)}")
