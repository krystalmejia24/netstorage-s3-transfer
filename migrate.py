import boto3
import hmac
import hashlib
import random
import time
import base64
import os
import threading
import logging
from akamai.netstorage import Netstorage, NetstorageError
from xml.etree import ElementTree as ETree
from smart_open import open as sopen
from dotenv import load_dotenv


logging.basicConfig(level=logging.ERROR,
                    format='(%(threadName)-10s) %(message)s',
                    )

env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

#netstorage 
root = "/{0}".format(os.getenv("NS_PATH"))
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
    return "s3://{0}/test/local{1}".format(s3_bucket, path)

def transfer(file=None):
    if file:
        file = "/{0}".format(file)
        url = "http://{0}{1}".format(ns_host, file)
        bucket = destination(file)
        try:
            with sopen(url, 'rb', transport_params=dict(headers=auth(file), buffer_size=1024*500)) as fin:
                with sopen(bucket, 'wb', transport_params=dict(session=session)) as fout:
                    while True:
                        buffer = fin.read(1024*2)
                        if not buffer:
                            fin.close()
                            break
                        else:
                            fout.write(buffer)
            transfered.append(file)
            files.remove(file[1:])
            semaphore.release()
        except Exception as e:
            print(e)

def manage_threads(file=False):
    if file:
        name = file.split('/')[-1]
        t = threading.Thread(name=name, target=transfer, args=(file,))
        threads.append(t)
    else:
        for thread in threads[:jobs]:
            thread.start()
            semaphore.acquire()
        for thread in threads[:jobs]:
            thread.join()
            threads.remove(thread)
        print("FILES IN QUEUE: {0}".format(len(files)))
        print("FILES TRANSFERED: {0}".format(len(transfered)))
        print("NUMBER OF ACTIVE THREADS: {0}".format(threading.active_count()))


def iterate(folder=""):
    list_opts = {
        'max_entries': jobs*10,
        'encoding': 'utf-8',
        'end': root + '0'
    }
    status, response = ns.list(folder, list_opts)
    tree = ETree.fromstring(response.content)
    try:
        resume = tree.find('resume').get('start')
        count = 0
        for child in tree:
            if child.get('type') == 'file':
                file = child.get('name')
                files.append(file) #TODO filter out based on bitrate
                manage_threads(file)
                count += 1
    except AttributeError:
        resume = None

    return resume

if __name__ == "__main__":
    try:
        dir = iterate(root)
        while True:
            while dir and len(files) < jobs: 
                dir = iterate(dir)
            if files:
                manage_threads()
            if not dir and not files:
                break
    except Exception as e:
        print(e)
    print('done')