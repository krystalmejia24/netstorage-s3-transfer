# netstorage-s3-transfer
Transfer of objects from Akamai's NetStorage to Amazon Web Services S3 storage. 

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


ENVIRONMENT VARIABLES:

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
