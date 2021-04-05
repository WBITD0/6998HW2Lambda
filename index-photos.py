import json
import datetime
import base64
import boto3
from aws_requests_auth.aws_auth import AWSRequestsAuth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import time

def lambda_handler(event, context):
    #TODO
    #index photo with labels and other stuff
    bucket = 'photostorageyz3691'
    session = boto3.Session()
    credentials = session.get_credentials()
    
    region = 'us-east-1'
    service = 'es'
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    img_suffixs = ['png','jpeg','jpg']
    host = 'search-photo-d2qhmk3lr2oojzxaw6rtnfk5ue.us-east-1.es.amazonaws.com'
    auth = AWSRequestsAuth(aws_access_key=access_key,
                   aws_secret_access_key=secret_key,
                   aws_token = credentials.token,
                   aws_host=host,
                   aws_region=region,
                   aws_service=service)
    es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection)
    
    record = event['Records']
    print("Records")
    print(record)
    obj_detection = boto3.client('rekognition')
    suffix = record[0]['s3']['object']['key'].split(".")[-1]
    if suffix not in img_suffixs:
        print("nothing to do here!")
        return {
            'statusCode': 200,
            'body': json.dumps('nothing to do here!')
        }
    labels = []
    s3 = boto3.resource('s3')
    s3_client = boto3.client("s3")
    for rec in record:
        
        name = rec['s3']['object']['key']
        label_name = 'Label/' + name.split('/')[1].split('.')[0] + '.txt'
        for i in range(60):
            try:
                file = s3.Object('photostorageyz3691', label_name).get()['Body'].read().decode("utf-8")
                print(file)
                if file == " ":
                    pass
                else:
                    customLabels = file.split(" ")
                    for customLabel in customLabels:
                        labels.append(customLabel.lower())
                s3_client.delete_object(Bucket = 'photostorageyz3691', Key = label_name)
                print('starting_label_prediction')
                response = obj_detection.detect_labels(Image={'S3Object': {'Bucket': bucket, 'Name': name}},
                                            MaxLabels=10)
                print('end_label_prediction')
                print('rekognition')
                print(response)
                for x in response['Labels']:
                    if x['Name'].lower() not in labels:
                        labels.append(x['Name'].lower())
                print('labels')
                print(labels)
                body = {"bucket":bucket,
                    "createdTimestamp":str(datetime.datetime.now()),
                    'labels':labels
                }
                es.index(index = 'photo', doc_type = "_doc", id = name, body = body)
                print("image processing finished")
                break
            except:
                time.sleep(1)
        if i == 60:
            return {
            'statusCode':400,
            "body":json.dumps("no corresponding txt file find")
                }
    return {
        'statusCode': 200,
        'body': json.dumps('finish indexing')
    }
            
           