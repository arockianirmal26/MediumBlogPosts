import json
import boto3
import jsonschema
import time
from jsonschema import validate
from datetime import datetime

s3_client = boto3.client('s3')
# Converting datetime object to string
dateTimeObj = datetime.now()

#format the string
timestampStr = dateTimeObj.strftime("%d-%b-%Y-%H%M%S,%f")


def lambda_handler(event, context):

    print("MyEvent:")
    print(event)
    method = event['context']['http-method']
    # method = event['http-method']
    print ("method IS", method)

    # Describe what kind of json you expect.
    trans_schema = {
        "type" : "object",
        "properties" : {
            "id" : {"type" : "number"},
            "trans_date_trans_time" : {"type" : "string"},
            "cc_num" : {"type" : "number"},
            "merchant" : {"type" : "string"},
            "category" : {"type" : "string"},
            "amt" : {"type" : "number"},
            "first" : {"type" : "string"},
            "last" : {"type" : "string"},
            "gender" : {"type" : "string"},
            "street" : {"type" : "string"},
            "city" : {"type" : "string"},
            "state" : {"type" : "string"},
            "zip" : {"type" : "number"},
            "lat" : {"type" : "number"},
            "long" : {"type" : "number"},
            "city_pop" : {"type" : "number"},
            "job" : {"type" : "string"},
            "dob" : {"type" : "string"},
            "trans_num" : {"type" : "string"},
            "unix_time" : {"type" : "number"},
            "merch_lat" : {"type" : "number"},
            "merch_long" : {"type" : "number"},
            "is_fraud" : {"type" : "number"}
        },
    }

    if method == "POST":

        p_record = event['body-json']
        print (p_record['id'])
        print (p_record)
        
        def validateJson(jsonData):
            try:
                # Validate will raise exception if given json is not
                validate(instance=jsonData, schema=trans_schema)
            except jsonschema.exceptions.ValidationError as err:
                return False
            return True
        
        isValid = validateJson(p_record)
        
        if isValid:
            print('p_record',p_record)
            print("Given JSON data is Valid")
            recordstring = json.dumps(p_record)
            print('recordstring',recordstring)
            
            
            client = boto3.client('kinesis',region_name='us-east-1', endpoint_url='https://kinesis.us-east-1.amazonaws.com/')
            response = client.put_record(
                StreamName='00-api-kinesis-data-stream',
                Data= recordstring,
                PartitionKey='string'
            )
            
            
        else:
            print('p_record',p_record)
            #validate(instance=p_record, schema=trans_schema)
            print("Given JSON data is InValid")
            
            # generate the name for the file with the timestamp to write to s3
            mykey = 'output-' + timestampStr + str(round(time.time() * 1000)) + '.txt'
            
            print(mykey)
            
            #put the file into the s3 bucket
            response = s3_client.put_object(Body=json.dumps(p_record), Bucket='00-s3-invalid-transactions', Key= mykey)
            

        
