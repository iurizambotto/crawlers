import base64
import time
from datetime import datetime

def take_screenshot(file_name, nav):
	time.sleep(.8)
	str_base64 = nav.get_screenshot_as_base64()
	open(file_name, "wb").write(base64.urlsafe_b64decode(str_base64))

	# client = storage.Client.from_service_account_json('./keys/ellox-web-scraping.json')     
	# bucket = client.get_bucket(self.cfg.get('GStorageSection', 'bucket.name'))
	# blob = bucket.blob(file_name)

	# # Uploading from local file without open()
	# blob.upload_from_string(data=base64.urlsafe_b64decode(str_base64), content_type="image/jpeg")
	print(f"File was saved: {file_name}")


def _get_date_filter(source, record, column):
	return {
		"itp": record.get(column) and record.get(column) != "",
		"tcp": record.get(column) and record.get(column) != "" and "-" not in record.get(column),
	}.get(source)

def convert_dt_objects(record, columns_mapping, str_pattern, source):
	for column, column_type in columns_mapping.items():
		# print(record, column)
		if column_type == "datetime":
			if _get_date_filter(source, record, column):
				record[column] = datetime.strptime(
					record.get(column), str_pattern
				)
			else:
				record[column] = None
			
	return record

