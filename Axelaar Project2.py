import boto3
import zipfile
from fsplit.filesplit import FileSplit
import os
import glob


'''for i in a:
    os.remove(os.listdir(i))'''

def download_file(bucketName,fileName,destFileName):
    s3 = boto3.client('s3')
    s3.download_file(bucketName,fileName,destFileName)
    #s3.download_file('axlpoc2','sample','Testing')


def unzipping_file(destFileName):
    with zipfile.ZipFile(destFileName, "r") as zip_ref:
        zip_ref.extractall("C:\\Users\\Lakshman\\Downloads\\unzipfolder")
    os.remove('C:\\Users\\Lakshman\\Downloads\\' + destFileName)


def split_file():
    files = os.listdir('C:\\Users\\Lakshman\\Downloads\\unzipfolder')
    for i in files:
        fs = FileSplit(i, splitsize=1684670, output_dir='C:\\Users\\Lakshman\\Downloads\\splitfiles')
        fs.split()
        os.remove('C:\\Users\\Lakshman\\Downloads\\unzipfolder\\'+i)
    #fs.split(include_header=True)

def zipping_file():
    files = os.listdir('C:\\Users\\Lakshman\\Downloads\\splitfiles')
    for i in files:
        with zipfile.ZipFile('zippingfile\\'+i+'.zip', 'w') as zip:
            zip.write('C:\\Users\\Lakshman\\Downloads\\splitfiles\\'+i)
            os.remove('C:\\Users\\Lakshman\\Downloads\\splitfiles\\'+i)


def upload_file():
    files = os.listdir('C:\\Users\\Lakshman\\Downloads\\zippingfile')
    for i in files:
        s3 = boto3.client('s3')
        s3.upload_file('C:\\Users\\Lakshman\\Downloads\\zippingfile\\'+i,'axlpoc2',i)





def final():
    bucketName = input('Enter Bucket Name :')
    fileName = input('Enter File Name :')
    destFileName = input('Enter Dest File name :')
    download_file(bucketName,fileName,destFileName)
    #download_file('axlpoc2', 'poc2.zip', 'mytest.zip')
    unzipping_file(destFileName)
    split_file()
    zipping_file()
    #upload_file()

final()


