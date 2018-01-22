from __future__ import print_function
import logging
import os
import io
import mimetypes
import json

from oauth2client.service_account import ServiceAccountCredentials
import httplib2
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from apiclient.http import MediaIoBaseDownload


SCOPES = ['https://www.googleapis.com/auth/drive']
APPLICATION_NAME = 'DB Backups'

logging.basicConfig()
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def build_service():
	try:
		scopes = SCOPES
		dir_path = os.path.dirname(os.path.realpath(__file__))

		credentials = ServiceAccountCredentials.from_json_keyfile_name(os.path.join(dir_path, 'xxxxxxxxxxxxxxxxx.json'), scopes=scopes)
		http_auth = credentials.authorize(httplib2.Http())
		service = build('drive', 'v3', http=http_auth)
		return service
	except Exception as e:
		logger.error('Failed to authenticate: %s: %s' % (e.__class__, e))

def list_files():
	"""
	Creates a Google Drive API service object and outputs the names and IDs
	for up to 10 files.
	"""
	service = build_service()

	results = service.files().list(pageSize=10, fields="nextPageToken, files(id, name)").execute()
	items = results.get('files', [])

	if not items:
		logger.info ('No files found.')
	else:
		logger.info ('Files:')
		for item in items:
			logger.info ('{0} ({1})'.format(item['name'], item['id']))

	return items

def upload_to_gdrive(file_path, filename):
	service = build_service()

	mimetype_upload_file = mimetypes.guess_type(file_path)
	# return a tuple mimetype: e.g ('text/csv', None)
	logger.info('%s: mimetype %s' % (file_path, mimetype_upload_file))

	file_mimetype = 'application/' + mimetype_upload_file[0]
	if file_mimetype is None:
		logger.info ('could not get mimetype - using the default')
		file_mimetype = '[*/*]'

	file_metadata = {
		'name': filename
	}

	try:
		media = MediaFileUpload(file_path,	mimetype=file_mimetype, resumable=True)
		file = service.files().create(body=file_metadata, media_body=media,	fields='id').execute()

		if file:
			logger.info ('Successfully uploaded file to Drive')
			logger.info ('File ID: %s' % file.get('id'))
	except Exception as e:
		logger.error ('Failed to upload: %s: %s' % (e.__class__, e))

def download_from_gdrive(filename):
	service = build_service()
	files = list_files()

	for file in files:
		if file['name'] == filename:
			file_id = file['id']

	request = service.files().get_media(fileId=file_id)
	fh = io.FileIO(filename, mode='wb')
	downloader = MediaIoBaseDownload(fh, request, chunksize=1024*1024)

	done = False
	while done is False:
		status, done = downloader.next_chunk()
		if status:
			logger.info ('Download %d%%.' % int(status.progress() * 100))

	logger.info ('Download Complete!')

#def main():
#	upload_to_gdrive('/file_path/dir/readonly_db_users.csv', 'readonly_db_users.csv')
#	list_files()
#	download_from_gdrive('readonly_db_users.csv')

# if __name__ == '__main__':
# 	main()
