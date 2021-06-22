import gzip
import requests
import shutil

def download_file(url,file_name):
    downloaded_obj = requests.get(url)
    with open(file_name, "wb") as file:
        file.write(downloaded_obj.content)

def un_zip(file_name,file_name_dest):
    with gzip.open(file_name, 'rb') as f_in:
        with open(file_name_dest, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

url = 'https://storage.googleapis.com/relevanc-data-jobs/randomized-transactions-202009.psv.gz'
file_name ='randomized-transactions-202009.psv.gz'
file_name_dest='randomized-transactions-202009.psv'
download_file(url,file_name)
un_zip(file_name,file_name_dest)
