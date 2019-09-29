import boto3
import zipfile
from fsplit.filesplit import FileSplit
import os
import glob
files = glob.glob('C:\\Users\\Lakshman\\Downloads\\splitfiletest1')
for i in files:
    os.remove(i)

'''for i in a:
    os.remove(os.listdir(i))'''

def download_file():
    s3 = boto3.client('s3')
    s3.download_file('axlpoc2','sample','Testing')

def unzipping_file():
    with zipfile.ZipFile("Poc22.zip", "r") as zip_ref:
        zip_ref.extractall("C:\\Users\\Lakshman\\Downloads")

def zipping_file():
    a = os.listdir('splitfiletest')
    print(a)

    with zipfile.ZipFile('zipped_file.zip', 'w') as zip:
        zip.write('C:\\Users\\Lakshman\\Downloads\\site_hits.tsv')

def upload_file():
    s3 = boto3.client('s3')
    s3.upload_file('poc22.zip','axlpoc2','upload_test2.zip')

def split_file():
    fs = FileSplit(file='C:\\Users\\Lakshman\\Downloads\\site_hits.tsv', splitsize=1684670, output_dir='C:\\Users\\Lakshman\\Downloads\\splitfiletest')
    fs.split()
    fs.split(include_header=True)



