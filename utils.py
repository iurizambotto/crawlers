import base64
import time


def take_screenshot(file_name, nav):
    time.sleep(0.8)
    str_base64 = nav.get_screenshot_as_base64()
    open(file_name, "wb").write(base64.urlsafe_b64decode(str_base64))

    # client = storage.Client.from_service_account_json('./keys/ellox-web-scraping.json')
    # bucket = client.get_bucket(self.cfg.get('GStorageSection', 'bucket.name'))
    # blob = bucket.blob(file_name)

    # # Uploading from local file without open()
    # blob.upload_from_string(data=base64.urlsafe_b64decode(str_base64), content_type="image/jpeg")
    print(f"File was saved: {file_name}")
