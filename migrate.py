import boto3
import hmac
import hashlib
import random
import time
import base64
import os
import threading
from akamai.netstorage import Netstorage, NetstorageError
from xml.etree import ElementTree as ETree
from sopen.smart_open import open as sopen
from dotenv import load_dotenv
from urllib.parse import quote
from ftplib import FTP

import logging
"""
logging.basicConfig(level=logging.DEBUG,
                    format='(%(threadName)-10s) %(message)s',
                    )
"""
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
ns = Netstorage(ns_host, keyname, key)

#s3
s3_bucket = os.getenv("S3_BUCKET")
session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

#threads
jobs = int(os.getenv("JOBS"))
semaphore = threading.Semaphore(jobs)
threads = []
files = []
transfered = []

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

def transfer(file=None):
    if file:
        file = "/{0}".format(file)
        url = "http://{0}{1}".format(ns_host, file)
        bucket = destination(file)
        with sopen(url, 'rb', transport_params=dict(headers=auth(file))) as fin:
            with sopen(bucket, 'wb', transport_params=dict(session=session)) as fout:
                manage_threads()
                for line in fin:
                    fout.write(line)
        transfered.append(file)
        print("FILES TRANSFERED COUNT: {0}".format(len(transfered)))
        print(f'Transfering {url} to {bucket}')
        semaphore.release()

def manage_threads(file=False): #TODO move threads to array to properly manage
    if file:
        name = file.split('/')[-1]
        t = threading.Thread(name=name, target=transfer, args=(file,))
        threads.append(t)
    else:
        print("NUMBER OF ACTIVE THREADS: {0}".format(threading.active_count()))

def iterate(folder=""):
    list_opts = {
        'max_entries': 1000,
        'encoding': 'utf-8',
        'end': root + '0'
    }
    status, response = ns.list(folder, list_opts)
    tree = ETree.fromstring(response.content)
    try:
        resume = tree.find('resume').get('start')
        for child in tree:
            if child.get('type') == 'file':
                file = child.get('name')
                files.append(file)
                manage_threads(file)
    except AttributeError:
        resume = None

    return resume

if __name__ == "__main__":
    dir = iterate(root)
    while True:
        if len(files) > jobs:
            for thread in threads:
                semaphore.acquire()
                thread.start() 
        if not iterate(dir):
            break
    print('done')