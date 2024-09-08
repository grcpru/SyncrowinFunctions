import azure.functions as func
import logging
import os
import requests
import pyodbc
import time

app = func.FunctionApp()

# Environment variables for OCR and SQL connection
OCR_ENDPOINT = os.environ.get("OCR_ENDPOINT")
OCR_KEY = os.environ.get("OCR_KEY")
DB_CONNECTION_STRING = os.environ.get("DB_CONNECTION_STRING")

@app.blob_trigger(arg_name="myblob", path="uploaded-files/{name}",
                  connection="syncrowinassetstorage_STORAGE") 
def blob_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob "
                 f"Name: {myblob.name} "
                 f"Blob Size: {myblob.length} bytes")

    # Read blob content
    blob_content = myblob.read()

    # Process the blob content with OCR
    ocr_result = process_ocr(blob_content)  # Send to OCR service
    extracted_text = extract_text(ocr_result)  # Extract text from OCR result
    
    # Save the extracted text to SQL database
    save_to_db(myblob.name, extracted_text)

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
    lines = [line["text"] for line in ocr_result["analyzeResult"]["readResults"][0]["lines"]]
    return "\n".join(lines)

def save_to_db(blob_name, extracted_text):
    """Saves the extracted text to Azure SQL Database."""
    try:
        # Establish connection to the Azure SQL Database
        connection = pyodbc.connect(DB_CONNECTION_STRING)
        cursor = connection.cursor()
        
        # Insert blob name and extracted text into the SQL table
        cursor.execute("INSERT INTO AssetData (FileName, ExtractedText) VALUES (?, ?)", (blob_name, extracted_text))
        
        # Commit the transaction
        connection.commit()

        # Close cursor and connection
        cursor.close()
        connection.close()

        logging.info(f"Saved data for {blob_name} to database successfully.")
    except Exception as e:
        logging.error(f"Error saving data to SQL Database: {str(e)}")
