import os
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Set the API key file path
API_KEY_FILE = r"C:\Users\Vrdella\Downloads\gdrive_credentials.json"

# Set the OAuth scope and redirect URI
SCOPES = ['https://www.googleapis.com/auth/drive.metadata.readonly']

# Create credentials using the API key file and OAuth
credentials = None
if os.path.exists('token.json'):
    credentials = Credentials.from_authorized_user_file('token.json')
if not credentials or not credentials.valid:
    if credentials and credentials.expired and credentials.refresh_token:
        credentials.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            API_KEY_FILE, SCOPES)
        credentials = flow.run_local_server(port=0)

    with open('token.json', 'w') as token:
        token.write(credentials.to_json())

# Build the Google Drive API service
drive_service = build('drive', 'v3', credentials=credentials)

# List folders in the root directory
results = drive_service.files().list(q="mimeType='application/vnd.google-apps.folder' and trashed=false",
                                      pageSize=10, fields="nextPageToken, files(id, name)").execute()
folders = results.get('files', [])

if not folders:
    print('No folders found.')
else:
    print('Folders:')
    for folder in folders:
        print(f'{folder["name"]} ({folder["id"]})')


