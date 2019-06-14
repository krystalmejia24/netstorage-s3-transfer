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
prism = {}

def setup_logger(name, file):
    logger = logging.getLogger(name)

    if name == "prism":
        fmt = '%(message)s'
    else:
        fmt = '%(asctime)s %(levelname)-4s %(threadName)-4s %(message)s'
    formatter = logging.Formatter(fmt=fmt, datefmt= '%m-%d %H:%M')

    fileHandler = logging.FileHandler(file, mode='a')
    fileHandler.setFormatter(formatter)

    logger.setLevel(logging.INFO)
    logger.addHandler(fileHandler)

def logger(msg, name, level='info'):
    if name == 'thread': 
        log = logging.getLogger('thread')
    if name == 'info': 
        log = logging.getLogger('info')
    if name == 'prism': 
        log = logging.getLogger('prism')

    if level == 'info':
        log.info(msg)
    if level == 'error':
        log.info(msg)

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
        return "s3://{0}{1}".format(s3_bucket, path)
    else:
        return path

def cleanup(file, mpx_id):
    global count
    transfered.append(file)
    logger('transfered {0}'.format(file), 'thread')
    prism[mpx_id].remove(file)
    if len(prism[mpx_id]) == 1: 
        logger(prism[mpx_id], 'prism')
        del prism[mpx_id]
    count -= 1
    semaphore.release()

def transfer(file, mpx_id):
    setup_logger("thread", "threads.log")
    global logger
    url = "http://{0}{1}".format(ns_host, file)
    bucket = destination(file)
    try:
        with sopen(url, 'rb', 1024*500, transport_params=dict(headers=auth(file))) as fin:
            with sopen(bucket, 'wb', transport_params=dict(session=session)) as fout:
                while True:
                    buffer = fin.read(1024*2)
                    if not buffer:
                        fin.close()
                        break
                    else:
                        fout.write(buffer)
        cleanup(file, mpx_id)
    except Exception as e:
        ##TODO REQUEUE ITEM if failed
        logger(e, 'threads', 'error')
        logger("Following was not transferred {0}".format(file), 'info', 'error')


def manage_threads(file=False, mpx_id=""):
    global count
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
        #TODO add log file with these info
        logger("FILES IN QUEUE: {0}".format(count), 'info')
        logger("FILES TRANSFERED: {0}".format(len(transfered)), 'info')



def generate_otfp(path, renditions): #renditions is a dictionary with a key,value of rendition,file
    otfp_url = None
    for i in sorted(renditions.keys(), reverse=True)[:3]:
        if not otfp_url:
            otfp_url = "{0}/{1},".format(destination(path, True), renditions[i].replace('.mp4', ''))
        else:
            otfp_url += "{0},".format(i)

    return ("{0}master.m3u8".format(otfp_url))

def filter_renditions(path, mpx_id):
    global count
    directory = "/" + '/'.join(path.split('/')[:-1])
    status, response = ns.dir(directory, {'encoding': 'utf-8'})
    tree = ETree.fromstring(response.content)
    renditions = {}
    otfp_url = {}

    for child in tree:
        if child.get('type') == 'file':
            file = child.get('name')
            r = file.split('_')[-1].replace(".mp4", "")
            renditions[int(r)] = file

    for i in sorted(renditions.keys(),  reverse=True)[:3]:
        file = "{0}/{1}".format(directory, renditions[i])
        prism[mpx_id].append(file)
        count += 1
        manage_threads(file, mpx_id)
    
    prism[mpx_id].append(generate_otfp(directory, renditions))


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
    global count
    count = 0
    setup_logger("info", "info.log")
    setup_logger("prism", "prism.log")
    try:
        dir = iterate(root+"/2019")
        while True:
            while dir and count < jobs: 
                dir = iterate(dir)
            if count:
                manage_threads()
            if not dir and not count:
                break
    except Exception as e:
        logger(e, 'info', 'error')
    logger('done', 'info')