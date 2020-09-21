from os import getenv, path
import io
import pickle
import pandas as pd

from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from smtplib import SMTP_SSL

from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

from helper import DTFormat, BASE_DIR

SCOPES = ['https://www.googleapis.com/auth/drive']


def gdrive_service():
    creds = None
    if path.exists(BASE_DIR+'/token.pickle'):
        with open(BASE_DIR+'/token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                BASE_DIR+'/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(BASE_DIR+'/token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return build('drive', 'v3', credentials=creds, cache_discovery=False)


def download_broadcast_config(verbose=True):
    if verbose: print('Downloading broadcast configuration from Google Drive...')
    service = gdrive_service()

    file_id = '1JhOsRI4D9qUpQ2e5aoV7bs40paK6Eg9iavVGGHS3txI'
    request = service.files().export_media(fileId=file_id, mimeType='text/csv')
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    drilldown_columns = ['country_code', 'gateway', 'operator_code',
                         'service_identifier1', 'service_identifier2']
    df = pd.read_csv(io.StringIO(fh.getvalue().decode('utf-8')),
                     dtype={col: 'str' for col in drilldown_columns})
    for col in df.columns:
        fill = '*' if col in drilldown_columns else '?'
        df[col] = df[col].fillna(fill)
    return df


def upload_to_google_drive(local_filename, gdrive_filename):
    service = gdrive_service()
    folder_id = '10QpFgtbBz5ncf3qdE1PbpPu0QGHcjTwg'

    query = "name='{}'".format(gdrive_filename)
    query += " and mimeType = 'application/vnd.google-apps.spreadsheet'"
    query += " and '{}' in parents".format(folder_id)
    results = service.files().list(q=query).execute()
    for file in results['files']:
        service.files().delete(fileId=file['id']).execute()

    file_metadata = {
        'name': gdrive_filename,
        'mimeType': 'application/vnd.google-apps.spreadsheet',
        'parents': [folder_id],
    }
    media = MediaFileUpload(
        local_filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        resumable=True
    )
    file = service.files().create(
        body=file_metadata, media_body=media, fields='id'
    ).execute()
    return file['id']


def send_email(start_date, local_filename, file_id):
    subject = '[BART] Broadcast review for the week of {}'.format(
        start_date.strftime(DTFormat.Y_M_D)
    )
    body = 'Report attached or available in Google Drive:\n\n'
    body += 'https://docs.google.com/spreadsheets/d/{}\n'.format(file_id)

    message = MIMEMultipart('alternative')
    message['To'] = getenv('EMAIL_TO')
    message['From'] = getenv('EMAIL_FROM')
    message['Subject'] = subject
    message.attach(MIMEText(body, 'plain'))

    p = MIMEBase('application', 'octet-stream')
    p.set_payload(open(local_filename, 'rb').read())
    encoders.encode_base64(p)
    p.add_header('Content-Disposition', 'attachment; filename="bart-{}.xlsx"'.format(
        start_date.strftime(DTFormat.Y_M_D)
    ))
    message.attach(p)

    s = SMTP_SSL(getenv('EMAIL_HOST'))
    s.login(getenv('EMAIL_USERNAME'), getenv('EMAIL_PASSWORD'))
    s.sendmail(
        message['From'],
        message['To'].split(','),
        message.as_string()
    )
    s.quit()
