import boto3
#import gzip

def upload_file():
        s3 = boto3.client('s3')
        s3.upload_file('C:\\Users\\Lakshman\\Downloads\\site_hits_test.tsv.txt.gz','axlpoc2','Lakshman/test.gz')

upload_file()
