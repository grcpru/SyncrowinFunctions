import azure.functions as func
import logging
import os
import requests
import pyodbc

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
    response = requests.post(f"{OCR_ENDPOINT}/vision/v3.2/read/analyze", headers=headers, data=blob_content)
    response.raise_for_status()  # Raise exception if OCR API fails
    return response.json()

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
