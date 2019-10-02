import boto3
import zipfile
from fsplit.filesplit import FileSplit
import os
import glob
import gzip


'''for i in a:
    os.remove(os.listdir(i))
    b = os.path.getsize()/4
    '''

def download_file(bucketName,fileName):
    s3 = boto3.client('s3')
    s3.download_file(bucketName,fileName,fileName)
    #s3.download_file('axlpoc2','sample','Testing')


def unzipping_file(fileName):
    with zipfile.ZipFile(fileName, "r") as zip_ref:
        zip_ref.extractall("unzipfolder")
    os.remove(fileName)

def split_file():
    files = os.listdir('unzipfolder')
    print ('reading file')
    file_size = os.path.getsize('unzipfolder/'+str(files[0]))/4
    print(file_size)
    for i in files:
        fs = FileSplit('unzipfolder/'+i, splitsize=file_size, output_dir='splitfiles')
        fs.split()
        os.remove('unzipfolder/'+i)
    #fs.split(include_header=True)

def zipping_file():
    files = os.listdir('splitfiles')
    for i in files:
        input = open('splitfiles/'+i,'rb')
        s = input.read()
        input.close()
        output = gzip.GzipFile('zippingfile/'+i+'.gz','wb')
        output.write(s)
        output.close()
        os.remove('splitfiles/' + i)

        '''with gzip.open('zippingfile/'+i+'.gz', 'w') as zip:
        #with zipfile.ZipFile('zippingfile/'+i+'.zip', 'w') as zip:
            zip.write('splitfiles/'+i)
            os.remove('splitfiles/'+i)'''


def upload_file(fileName):
    folder = fileName.split('.')
    folderName = folder[0]
    files = os.listdir('zippingfile')
    for i in files:
        f = i.split('.')
        name = f[0]
        s3 = boto3.client('s3')
        s3.upload_file('zippingfile/'+i,'axlpoc2',folderName+'/'+name+'.gz')

        os.remove('zippingfile/'+i)





def final():
    bucketName = input('Enter Bucket Name :')
    #print ('Success')
    fileName = input('Enter File Name :')
    #print('success')
    #destFileName = input('Enter Dest File name :')
    #dir = 'C:\\Users\\Lakshman\\Downloads\\'
    #dir = "\\home\\ec2-user\\AWS-POC2\\"


    #download_file('axlpoc2','AxelaarPoc2.zip')
    download_file(bucketName,fileName)
    unzipping_file(fileName)
    split_file()
    zipping_file()
    upload_file(fileName)

final()


