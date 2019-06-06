import boto3
import hmac
import hashlib
import random
import time
import base64
import os
import threading
from sopen.smart_open import open as sopen
from akamai.netstorage import Netstorage, NetstorageError
from dotenv import load_dotenv
from urllib.parse import quote
from ftplib import FTP

env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

#ftp
root = "/{0}".format(os.getenv("NS_PATH"))
ftp = FTP(
    os.getenv("FTP_HOST"),
    os.getenv("FTP_USR"),
    os.getenv("FTP_PWD")
)

#netstorage
ns_host = os.getenv("NS_HOST")
key = os.getenv("NS_KEY")
keyname = os.getenv("NS_KEYNAME")

#s3
s3_bucket = os.getenv("S3_BUCKET")
session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

#threads
semaphore = threading.Semaphore(5)

def auth(path):
    #path = quote(path)
    acs_action = 'version=1&action=download'
    acs_auth_data = "5, 0.0.0.0, 0.0.0.0, {0}, {1}, {2}".format(
        int(time.time()), 
        str(random.getrandbits(32)), 
        keyname)
    sign_string = "{0}\nx-akamai-acs-action:{1}\n".format(path, acs_action)
    message = acs_auth_data + sign_string
    digest = hmac.new(key.encode(), message.encode(), hashlib.sha256).digest()
    acs_auth_sign = base64.b64encode(digest)

    headers = { 
        'X-Akamai-ACS-Action': acs_action,
        'X-Akamai-ACS-Auth-Data': acs_auth_data,
        'X-Akamai-ACS-Auth-Sign': acs_auth_sign,
    }

    return headers

def destination(file):
    path = file.replace(root, "")
    return "s3://{0}/test{1}".format(s3_bucket, path)

def transfer(file=""):
    url = "http://{0}{1}".format(ns_host, file)
    bucket = destination(file)
    with sopen(url, 'rb', transport_params=dict(headers=auth(file))) as fin:
        with sopen(bucket, 'wb', transport_params=dict(session=session)) as fout:
          print(f'Transfering {url} to {bucket}')
          for line in fin:
            fout.write(line)
    semaphore.release()

def find(folder=""):
    contents = ftp.nlst(folder)
    contents.sort(reverse=True)
    return contents


def start(folder=""):
    for item in find(folder):
        dir = "{0}/{1}".format(folder, item)
        if ".mp4" not in item:
            print(f'No files in {dir}')
            start(dir)
        else:
            semaphore.acquire()
            t = threading.Thread(target=transfer, args=(dir,))
            t.start()


if __name__ == "__main__":
    start(root)