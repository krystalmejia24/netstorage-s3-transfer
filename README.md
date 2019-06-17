# netstorage-s3-transfer
Transfer of objects from Akamai's NetStorage to Amazon Web Services S3 storage. This tool makes use of unique requirements for CBS Sports Digital. 

## Description

This tool was built to help migrate content from NetStorage to S3. It makes use of akamai's HTTP NetStorage [API](https://github.com/akamai/NetStorageKit-Python) to iterate and filter mp4 files based on the different renditions. The files are iterated through in [lexigraphical order](https://learn.akamai.com/en-us/webhelp/netstorage/netstorage-http-api-developer-guide/GUID-39E4FFE2-CF0E-474A-B91D-FA0758DB174A.html#GUID-39E4FFE2-CF0E-474A-B91D-FA0758DB174A), so if your content is stored with dates in its path, oldest content will be transfered first. For transfering of objects, the tool uses a [forked](https://github.com/krystalmejia24/smart_open) verion of [smart open](https://github.com/RaRe-Technologies/smart_open) to allow authentication headers when reading files from NetStorage. 

The tool will iterate through NetStorage and queue up files as they are filtered for rendtions. It will continute this process until it finds a sufficient number of [`jobs`](#environment-variables) to transfer. A thread is spun up for each transfer, with semaphores being used to control the number of threads in execution does not exceed the number of jobs. Once all the threads are cleaned up, iteration continues unless there are enough files in the queue to kick off another set of jobs. Any jobs that fail to transfer are simply added to the end of queue for future transfer.


## Install

Install dependencies through pip
```bash
pip install -r requirements.txt
```

This application loads environment variables from .env file. You can copy the example in the repository.
```bash
cp .env.example .env
```

### Configuration

You will need to provision both NetStorage and S3 for access.

For Akamai, you will need the to configure your account for HTTP access, which means creating an Upload Account (key is provided on creation.) Refer [here](https://learn.akamai.com/en-us/webhelp/netstorage/netstorage-http-api-developer-guide/GUID-9A24E3A2-F989-4E3B-869F-B536635B0449.html) for any additional help on Akamai configuration. 

For s3, please create an IAM policay to give the application proper access to your S3 bucket. Refer [here](https://aws.amazon.com/blogs/security/writing-iam-policies-how-to-grant-access-to-an-amazon-s3-bucket/) for any additional help on creating your policies. 


### ENVIRONMENT VARIABLES:

| Environment variable | Description |
| -------------------- | ----------- |
| NS_HOST | NetStorage HTTP API hostname
| NS_PATH | root directory of the account you want to transfer. This should include your Akamai CP CODE 
| NS_KEY_NAME | The Upload Account Name
| NS_KEY | The HTTP API Key given to you on create of your upload account
| S3_BUCKET | Destination bucket of your files
| AWS_ACCESS_KEY_ID | AWS credentials
| AWS_SECRET_ACCESS_KEY | AWS credentials
| JOBS | Number of Threads used to transfer your files. One thread is used per file transfer


## Running

To transfer objects under the NS_PATH, simply run the script

```bash
python migrate.py
```

If you wish to migrate subdirectories within the NS_PATH
```bash
python migrate.py start [optional_sub_directory]
```

The directory passed as an argument **must** be found under the NS_PATH directory. Only objects within this subdirectory will be transfered. 

If the server running the transfer dies unexpectedly, you can resume transfer of objects by passing through the last transfered object. You should be able to grab the file found in your logs. 

```bash
python migrate.py resume [absolute_path_to_file]
```
