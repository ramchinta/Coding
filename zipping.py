import zipfile
import os
def zipping_file():
    files = os.listdir('C:\\Users\\Lakshman\\Downloads\\splitfiletest')
    for i in files:
        with zipfile.ZipFile('zippingfile\\'+i+'.zip', 'w') as zip:
            zip.write('splitfiletest\\'+i)

zipping_file()