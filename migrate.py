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

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(threadName)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M',
                    filename='logs/temp.log',
                    filemode='w')

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
transfered = []
files = {}
prism = {}

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

def destination(file, otfp=False):
    path = file.replace(root, "")
    if not otfp:
        return "s3://{0}/test/local{1}".format(s3_bucket, path)
    else:
        return path

def cleanup(file, mpx_id):
    transfered.append(file)
    del files[file]
    prism[mpx_id].remove(file)
    print(len(prism[mpx_id]))
    if mpx_id not in files.values() and len(prism[mpx_id]) == 1: 
        print(prism[mpx_id]) #TODO ADD TO LOG FILE AND DELETE KEY
    semaphore.release()

def transfer(file, mpx_id):
    url = "http://{0}{1}".format(ns_host, file)
    bucket = destination(file)
    try:
        with sopen(url, 'rb', 1024*500, transport_params=dict(headers=auth(file))) as fin:
            with sopen(bucket, 'wb', transport_params=dict(session=session)) as fout:
                while True:
                    buffer = fin.read(1024)
                    if not buffer:
                        fin.close()
                        break
                    else:
                        fout.write(buffer)
        cleanup(file, mpx_id)
    except Exception as e:
        ##TODO REQUEUE ITEM if failed
        print(e)

def manage_threads(file=False, mpx_id=""):
    if file:
        name = file.split('/')[-1]
        t = threading.Thread(name=name, target=transfer, args=(file,mpx_id,))
        threads.append(t)
    else:
        for thread in threads[:jobs]:
            thread.start()
            semaphore.acquire()
        for thread in threads[:jobs]:
            thread.join()
            threads.remove(thread)
        #TODO add log file with these stats
        print("FILES IN QUEUE: {0}".format(len(files.keys())))
        print("FILES TRANSFERED: {0}".format(len(transfered)))

def filter_renditions(path, mpx_id):
    directory = "/" + '/'.join(path.split('/')[:-1])
    status, response = ns.dir(directory, {'encoding': 'utf-8'})
    tree = ETree.fromstring(response.content)
    renditions = {}
    otfp_url = None 

    for child in tree:
        if child.get('type') == 'file':
            file = child.get('name')
            r = file.split('_')[-1].replace(".mp4", "")
            renditions[int(r)] = file

    for i in sorted(renditions.keys(), reverse=True)[:3]:
        file = "{0}/{1}".format(directory, renditions[i])
        files[file] = mpx_id
        prism[mpx_id].append(file)
        manage_threads(file, mpx_id)
        if not otfp_url:
            otfp_url = "{0}/{1},".format(destination(directory, True), renditions[i].replace('.mp4', ''))
        else:
            otfp_url += "{0},".format(i)

    prism[mpx_id].append("{0}master.m3u8".format(otfp_url))


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
        for child in tree:
            if child.get('type') == 'file':
                file = child.get('name')
                mpx_id = file.split('/')[-2]
                if mpx_id not in prism:
                    prism[mpx_id] = []
                    filter_renditions(file, mpx_id)
    except AttributeError:
        resume = None

    return resume

if __name__ == "__main__":
    try:
        dir = iterate(root)
        while True:
            while dir and len(files.keys()) < jobs: 
                dir = iterate(dir)
            if files:
                manage_threads()
            if not dir and not files:
                break
    except Exception as e:
        print(e)
    print('done')