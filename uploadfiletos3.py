import boto3
#import gzip

def upload_file():
        s3 = boto3.client('s3')
        s3.upload_file('C:\\Users\\Lakshman\\Downloads\\site_hits_test.zip','axlpoc2','test.zip')

upload_file()
