import json
import boto3
from aws_requests_auth.aws_auth import AWSRequestsAuth
from elasticsearch import Elasticsearch, RequestsHttpConnection
import random
import time

def lambda_handler(event, context):
    credentials = boto3.Session().get_credentials()
    response = dict()
    
    response["headers"] = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET,PUT",
        "Access-Control-Allow-Headers": "Content-Type, Access-Control-Allow-Headers, Authorization, X-Requested-With"
    }
    #response["headers"] = {"Access-Control-Allow-Origin": "*"}
    
    print(event)
    if event["httpMethod"].upper() == "OPTIONS":
            response['statusCode'] = 200

            return response
    input_type = event["queryStringParameters"]['q'].split("/")[-1]
    query = "/".join(event["queryStringParameters"]['q'].split("/")[:-1])
    print(query)
    if input_type not in ["audio","text"]:
        response['statusCode'] = 400
        response['body'] = json.dump("invalid input")
    if input_type == "audio":
        transcribe = boto3.client('transcribe')
        file_location = query
        url = 'https://s3.amazonaws.com/photostorageyz3691/{}'.format(file_location)
        print(url)
        job_name = str(random.random())
        #job_name = file_location.split('/')[1].split('.')[0]
        re = transcribe.start_transcription_job(
            TranscriptionJobName = job_name,
            LanguageCode='en-US',
            MediaFormat='webm',
            Media={
                'MediaFileUri': url
            },
            OutputBucketName='photostorageyz3691' 
            )
        print("response=",re)
        s3 = boto3.resource('s3')
        s3_client = boto3.client("s3")
        #buc = s3.Bucket('photostorageyz3691')
        print(job_name+".json")
        for i in range(60):
            try:
                file = s3.Object('photostorageyz3691', job_name+".json")
                file_content = file.get()['Body'].read().decode('utf-8')
                json_content = json.loads(file_content)
                print(json_content)
                text = json_content['results']['transcripts'][0]['transcript']
                response['statusCode'] = 200
                response['body'] = json.dumps(text)
                #response['body'] = text
                print(response)
                s3_client.delete_object(Bucket = 'photostorageyz3691', Key = job_name+".json")
                return response
            except Exception as e:
                time.sleep(1)
        response['statusCode'] = 400
        response['body'] = json.dumps("transcribe failed to finish its job in 60s")
        return response
        
    print('Query:', query)
    lex = boto3.client('lex-runtime')
    lex_response = lex.post_text(
            botName='Search',
            botAlias='search',
            userId='01',
            inputText=query
        )
    print("Lex", lex_response)
    keywords = []
    all_value = lex_response['slots'].values()
    for val in all_value:
        if val is None:
            continue
        if val[-1] == 's':
            keywords.append(val[:-1])
        else:
            keywords.append(val)
    host = 'search-photo-d2qhmk3lr2oojzxaw6rtnfk5ue.us-east-1.es.amazonaws.com'
    index = 'photo'
    region = 'us-east-1'
    service = 'es'
    access_key = credentials.access_key
    secret_key = credentials.secret_key
    
    awsauth = AWSRequestsAuth(aws_access_key = access_key,
                       aws_secret_access_key = secret_key,
                       aws_token = credentials.token,
                       aws_host = host,
                       aws_region = region,
                       aws_service = service)

    
    es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)   
    #response['results'] = []
    urls = []
    
    for keyword in keywords:
        r = es.search(index="photo", body={"query": {"match" : { "labels" : keyword }}}, size = 5)
        print(r)
        items = r['hits']['hits']
        print(items)
        for item in items:
            bucket = item["_source"]["bucket"]
            object_key = item["_id"]
            url = "https://{:s}.s3.amazonaws.com/{:s}".format(bucket, object_key)
            #img_info = {"url":url,"labels":["cat","pet"]}
            if url not in urls:
                urls.append(url)
    
    response['body'] = json.dumps(urls)
    response['statusCode'] = 200
    #response["SearchResponse"]={"results": [{"url":url,"labels":["cat","pet"]}]}
    #response["results"]:[{"url":url,"labels":["cat","pet"]}]
    print("response")
    print(response)
    return response
        
        
    