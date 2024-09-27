import json
import os
import requests
import boto3
import zipfile
from io import BytesIO

# Initialize S3 client
s3 = boto3.client('s3')

# Airtable configurations
AIRTABLE_API_KEY = os.environ['AIRTABLE_API_KEY']
AIRTABLE_BASE_ID = os.environ['AIRTABLE_BASE_ID']
AIRTABLE_TABLE_NAME = os.environ['AIRTABLE_TABLE_NAME']
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']

# Airtable API URL
AIRTABLE_URL = f'https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}'

def get_record(record_id):
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
        'Content-Type': 'application/json'
    }
    response = requests.get(f'{AIRTABLE_URL}/{record_id}', headers=headers)
    response.raise_for_status()
    return response.json()

def download_attachment(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.content

def create_zip_file(attachments1, attachments2):
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        if attachments1:
            folder_name_1 = 'Pictures'
            for attachment in attachments1:
                file_content = download_attachment(attachment['url'])
                zip_file.writestr(os.path.join(folder_name_1, attachment['filename']), file_content)
        
        if attachments2:
            folder_name_2 = 'TestResults'
            for attachment in attachments2:
                file_content = download_attachment(attachment['url'])
                zip_file.writestr(os.path.join(folder_name_2, attachment['filename']), file_content)
    
    zip_buffer.seek(0)
    return zip_buffer

def upload_zip_to_s3(zip_buffer, zip_file_name):
    s3_key = f'{zip_file_name}.zip'
    s3.upload_fileobj(zip_buffer, S3_BUCKET_NAME, s3_key)
    return s3.generate_presigned_url('get_object', Params={'Bucket': S3_BUCKET_NAME, 'Key': s3_key}, ExpiresIn=3600)

def update_airtable_record(record_id, zip_file_url):
    update_url = f'{AIRTABLE_URL}/{record_id}'
    headers = {
        'Authorization': f'Bearer {AIRTABLE_API_KEY}',
        'Content-Type': 'application/json'
    }
    data = {
        'fields': {
            'Zip': [{'url': zip_file_url}],
        }
    }
    response = requests.patch(update_url, headers=headers, json=data)
    response.raise_for_status()

def lambda_handler(event, context):
    # Extract the record ID from the event
    record_id = event['pathParameters']['recordId']
    
    # Fetch the record from Airtable
    record = get_record(record_id)
    fields = record.get('fields', {})
    attachments1 = fields.get('Pictures', [])
    attachments2 = fields.get('TestResults', [])
    zip_file_name = fields.get('ZipFileName', 'attachments')

    # Create the ZIP file
    zip_buffer = create_zip_file(attachments1, attachments2)

    # Upload the ZIP file to S3 and get the signed URL
    zip_file_url = upload_zip_to_s3(zip_buffer, zip_file_name)

    # Update the Airtable record with the signed URL
    update_airtable_record(record_id, zip_file_url)

    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Processing completed successfully.'})
    }
