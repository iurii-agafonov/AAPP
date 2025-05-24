from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload, MediaIoBaseUpload
from google.auth.transport.requests import Request

import os
import json
import io
import pandas as pd
from datetime import datetime, timedelta
#from google.cloud import secretmanager
import pytz
import joblib

'''def get_service_account_key(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/812485502403/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

service_account_file = get_service_account_key("SERVICE_ACCOUNT_KEY")'''

# Path to your service account credentials JSON file
#service_account_file = '/Users/iuriiagafonov/Documents/PERSONAL PROJECTS/INFOCOM/peace_process/cloud collection/peaceprocess-0b61b099c89e.json'
service_account_file = 'peaceprocess-0b61b099c89e.json'
#service_account_file = json.loads(os.environ["SERVICE_ACCOUNT_KEY"])
#print("All environment variables:", os.environ)
#service_account_file = os.environ.get("SERVICE_ACCOUNT_KEY")
print(f"Service account file: {service_account_file}")

# Authenticate with service account credentials
SCOPES = ['https://www.googleapis.com/auth/drive']
credentials = service_account.Credentials.from_service_account_file(service_account_file, scopes=SCOPES)

# Build the Drive API service
service = build('drive', 'v3', credentials=credentials)

def upload_dataframe_to_drive_as_new_file(df, folder_id, file_prefix='new_file', timezone='UTC'):
    # Set the time zone using pytz
    tz = pytz.timezone(timezone)
    current_date = datetime.now(tz).strftime('%Y-%m-%d')  # Format the date in the specified time zone
    
    # Generate the file name
    file_name = f"{file_prefix}_{current_date}.xlsx"
    
    # Save the DataFrame to a temporary file
    temp_file = 'temp_file.xlsx'
    df.to_excel(temp_file, index=False)
    
    # Create a MediaFileUpload object for the file
    media = MediaFileUpload(temp_file, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    
    # Define the metadata for the new file
    file_metadata = {
        'name': file_name,  # Name of the file on Google Drive
        'parents': [folder_id]  # The folder where the file will be saved
    }
    
    # Upload the new file to Google Drive
    new_file = service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id, name'
    ).execute()

    print(f"New Excel file '{new_file['name']}' with ID '{new_file['id']}' has been uploaded.")
    
    # Remove the temporary file
    os.remove(temp_file)

def download_file_to_dataframe(file_id):
    # Create a request to get the file content
    request = service.files().get_media(fileId=file_id)
    
    # Create a BytesIO buffer to hold the file content
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    
    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(f"File has been downloaded")

    # Seek to the beginning of the buffer
    buffer.seek(0)

    # Load the buffer into a pandas DataFrame
    # Adjust read_csv or other method based on your file format
    df = pd.read_excel(buffer)
    
    return df

def upload_dataframe_to_drive(df, file_id, file_name='updated_file.xlsx'):
    # Save the DataFrame to a temporary file
    temp_file = 'temp_file.xlsx'
    df.to_excel(temp_file, index=False)
    
    # Create a MediaFileUpload object for the file
    media = MediaFileUpload(temp_file, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    
    # Update the existing file on Google Drive
    updated_file = service.files().update(
        fileId=file_id,
        media_body=media,
        fields='id, name'
    ).execute()

    print(f"Excel file '{updated_file['name']}' with ID '{updated_file['id']}' has been uploaded.")
    
    # Remove the temporary file
    os.remove(temp_file)


def download_txt_file(file_id):
    # Create a request to get the file content
    request = service.files().get_media(fileId=file_id)
    
    # Create a BytesIO buffer to hold the file content
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)
    
    done = False
    while not done:
        status, done = downloader.next_chunk()
        print(f"Txt file has been downloaded")

    # Seek to the beginning of the buffer
    buffer.seek(0)

    # Read the content as text
    content = buffer.read().decode('utf-8')
    
    # Extract the date from the content
    date_str = content.strip().replace("-", "/")  # Assuming the content is a single line with date and time
    date = datetime.strptime(date_str, "%d/%m/%Y %H:%M:%S")

    return date

def upload_txt_file(file_id, content):
    # Write the updated content back to a BytesIO buffer
    buffer = io.BytesIO()
    buffer.write(content.encode('utf-8'))
    buffer.seek(0)

    # Create a media file upload object
    media = MediaIoBaseUpload(buffer, mimetype='text/plain')

    # Update the file on Google Drive
    file = service.files().update(fileId=file_id, media_body=media).execute()
    print(f"Txt file with ID {file['id']} has been uploaded")



def read_json_from_drive(folder_id_json, file_name):
    # Search for the file in the specified folder
    query = f"'{folder_id_json}' in parents and name='{file_name}' and trashed=false"
    response = service.files().list(q=query, fields="files(id)").execute()
    files = response.get('files', [])

    if not files:
        print(f"File '{file_name}' not found in the specified folder.")
        return None

    file_id = files[0]['id']
    
    # Download the file content
    request = service.files().get_media(fileId=file_id)
    file_stream = io.BytesIO()
    downloader = MediaIoBaseDownload(file_stream, request)
    
    done = False
    while not done:
        _, done = downloader.next_chunk()
    
    # Move to the beginning of the stream and load JSON
    file_stream.seek(0)
    json_data = json.load(file_stream)

    print(f"Successfully read '{file_name}' from Google Drive.")
    return json_data



# Save the JSON file to Google Drive
def save_json_to_drive(json_data, folder_id_json, file_name):
    # Save JSON data to a temporary file
    temp_file_path = '/tmp/temp_file.json'
    with open(temp_file_path, 'w', encoding='utf-8') as file:
        json.dump(json.loads(json_data), file, ensure_ascii=False, indent=4)

    # Search for the file in the specified folder
    query = f"'{folder_id_json}' in parents and name='{file_name}' and trashed=false"
    response = service.files().list(q=query, fields="files(id)").execute()
    files = response.get('files', [])

    # If the file exists, update it; otherwise, create a new file
    if files:
        file_id = files[0]['id']
        media = MediaFileUpload(temp_file_path, mimetype='application/json')
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"Updated existing file: {file_name}")
    else:
        # Define the metadata for the new file
        file_metadata = {
            'name': file_name,  # Name of the file on Google Drive
            'parents': [folder_id_json]  # The folder where the file will be saved
        }
        media = MediaFileUpload(temp_file_path, mimetype='application/json')
        service.files().create(body=file_metadata, media_body=media).execute()
        print(f"Created new file: {file_name}")

    # Remove the temporary file
    os.remove(temp_file_path)