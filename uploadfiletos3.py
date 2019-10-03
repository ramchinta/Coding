import boto3
#import gzip

def upload_file():
        s3 = boto3.client('s3')
        s3.upload_file('C:\\Users\\Lakshman\\Downloads\\aa.zip','axlpoc2','input_files/aa.zip')

upload_file()
