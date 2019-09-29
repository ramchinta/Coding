import zipfile
with zipfile.ZipFile("Poc22.zip","r") as zip_ref:
    zip_ref.extractall("C:\\Users\\Lakshman\\Downloads\\test")