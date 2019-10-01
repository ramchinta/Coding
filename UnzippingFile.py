import zipfile
import os
from fsplit.filesplit import FileSplit
import boto3

s3 = boto3.client('s3')
s3.upload_file('C:\\Users\\Lakshman\\Documents\\GitHub\\Coding\\copy_command.py','axlpoc2','copy_command.py')



'''
a = input('enter file name:')
b=a.split('.')
print(b[0])







def split_file(dir):
    files = os.listdir(dir+'unzipfolder')
    file_size = os.path.getsize(dir+'unzipfolder\\'+str(files[0]))/4
    print(file_size)
    for i in files:
        fs = FileSplit(dir+'unzipfolder\\'+i, splitsize=file_size, output_dir=dir+'splitfiles')
        fs.split()

split_file('C:\\Users\\Lakshman\\Downloads\\')





dir = 'C:\\Users\\Lakshman\\Downloads\\'
files = os.listdir(dir + 'unzipfolder')
file_size = os.path.getsize(dir + 'unzipfolder\\'+ files[0]) / 4
print(file_size)


file_size = os.path.getsize('C:\\Users\\Lakshman\\Downloads\\'+'unzipfolder\\site_hits.tsv')/4
print(file_size)

with zipfile.ZipFile("Poc22.zip","r") as zip_ref:
    zip_ref.extractall("C:\\Users\\Lakshman\\Downloads\\test")
'''