import json
import base64
import boto3

from datetime import datetime

def lambda_handler(event, context):

    
    client = boto3.client('rds-data')
    db_cluster_arn = "XXXXXXX"
    db_credentials_secrets_store_arn = "XXXXX"
    database_name = "DB NAME"

    #print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        t_record = base64.b64decode(record['kinesis']['data'])

        # decode the bytes into a string
        str_record = str(t_record,'utf-8')

        #transform the json string into a dictionary
        dict_record = json.loads(str_record)
        print(dict_record)
        
        def execute_statement(sql, transaction_id=None):
            parameters = {
                'secretArn': db_credentials_secrets_store_arn,
                'database': database_name,
                'resourceArn': db_cluster_arn,
                'sql': sql
            }
            if transaction_id is not None:
                parameters['transactionId'] = transaction_id
            response = client.execute_statement(**parameters)
            return response
        
        
        transaction = client.begin_transaction(
            secretArn=db_credentials_secrets_store_arn,
            resourceArn=db_cluster_arn,
            database=database_name)
            
        try:
            
            
            
            #------------insert into 'CITY' table 
            sql ="insert into CITY(CITY_NAME, STATE, CITY_POPULATION) values ('" + dict_record['city'] + "','" + dict_record['state'] + "', '" + str(dict_record['city_pop']) + "' ) ON DUPLICATE KEY UPDATE CITY_NAME = VALUES(CITY_NAME), STATE = VALUES(STATE), CITY_POPULATION = VALUES(CITY_POPULATION)"
            response = execute_statement( sql,transaction['transactionId'] )
            print(transaction['transactionId'])
            
        
            #------------insert/update into 'ADDRESS' table 
            sql ="insert into ADDRESS(STREET, ZIP, LAT, LONGI) values ('" + dict_record['street'] + "','" + str(dict_record['zip']) + "', '" + str(dict_record['lat']) + "' , '" + str(dict_record['long']) + "') ON DUPLICATE KEY UPDATE STREET = VALUES(STREET), ZIP = VALUES(ZIP), LAT = VALUES(LAT) , LONGI = VALUES(LONGI)"
            response = execute_statement( sql,transaction['transactionId'] )
            
        
            sql ="update ADDRESS a inner join CITY c set a.CITY_ID = c.CITY_ID where a.STREET='" + dict_record['street'] + "' and a.ZIP=" + str(dict_record['zip']) + " and c.CITY_NAME='"+ dict_record['city'] + "' and c.STATE='"+ dict_record['state'] + "'"
            response = execute_statement( sql,transaction['transactionId'] )
            
            #------------insert/update into 'CUSTOMER' table 

            sql ="insert into CUSTOMER(FIRST_NAME, LAST_NAME, CREDIT_CARD_NUMBER, GENDER, JOB, DATE_OF_BIRTH) values ('" + dict_record['first'] + "','" + dict_record['last'] + "', '" + str(dict_record['cc_num']) + "' , '" + dict_record['gender'] + "', '" + dict_record['job'] + "', '" + dict_record['dob'] + "') ON DUPLICATE KEY UPDATE FIRST_NAME = VALUES(FIRST_NAME), LAST_NAME = VALUES(LAST_NAME), CREDIT_CARD_NUMBER = VALUES(CREDIT_CARD_NUMBER) , GENDER = VALUES(GENDER) , JOB = VALUES(JOB), DATE_OF_BIRTH = VALUES(DATE_OF_BIRTH)"
            response = execute_statement( sql,transaction['transactionId'] )
            
            
            sql ="update CUSTOMER c inner join ADDRESS a set c.ADDR_ID = a.ADDR_ID where c.CREDIT_CARD_NUMBER='" + str(dict_record['cc_num']) + "' and a.STREET='" + dict_record['street'] + "' and a.ZIP=" + str(dict_record['zip']) + ""
            response = execute_statement( sql,transaction['transactionId'] )
           
            #------------insert into 'MERCHANT' table 
            
            sql ="insert into MERCHANT(MERCHANT_NAME, CATEGORY, LAT, LONGI) values ('" + dict_record['merchant'] + "','" + dict_record['category'] + "', '" + str(dict_record['merch_lat']) + "' , '" + str(dict_record['merch_long']) + "') ON DUPLICATE KEY UPDATE MERCHANT_NAME = VALUES(MERCHANT_NAME), CATEGORY = VALUES(CATEGORY), LAT = VALUES(LAT) , LONGI = VALUES(LONGI)"
            response = execute_statement( sql,transaction['transactionId'] )
            
        
            #------------insert/update into 'TRANSACTION' table 
            
            sql ="insert into TRANSACTION(TRANSACTION_ID, AMOUNT, TRANSFER_TIMESTAMP, UNIX_TIME, IS_FRAUD) values ('" + dict_record['trans_num'] + "','" + str(dict_record['amt']) + "','" + str(dict_record['trans_date_trans_time']) + "', '" + str(dict_record['unix_time']) + "' , '" + str(dict_record['is_fraud']) + "') ON DUPLICATE KEY UPDATE TRANSACTION_ID = VALUES(TRANSACTION_ID), AMOUNT = VALUES(AMOUNT), TRANSFER_TIMESTAMP = VALUES(TRANSFER_TIMESTAMP) , UNIX_TIME = VALUES(UNIX_TIME) , IS_FRAUD = VALUES(IS_FRAUD)"
            response = execute_statement( sql,transaction['transactionId'] )
            

            sql ="update TRANSACTION t inner join CUSTOMER c set t.CUST_ID = c.CUST_ID where t.TRANSACTION_ID='" + dict_record['trans_num']+ "' and c.CREDIT_CARD_NUMBER='" + str(dict_record['cc_num']) + "'"
            response = execute_statement( sql,transaction['transactionId'] )
            
        
        
            sql ="update TRANSACTION t inner join MERCHANT m set t.MERCHANT_ID = m.MERCHANT_ID where t.TRANSACTION_ID='" + dict_record['trans_num']+ "' and m.MERCHANT_NAME='" + dict_record['merchant']+ "' and m.CATEGORY='" + dict_record['category']+ "'"
            response = execute_statement( sql,transaction['transactionId'] )
            
            
        except Exception as e:
            print(f'Error: {e}')
            transaction_response = client.rollback_transaction(
                secretArn=db_credentials_secrets_store_arn,
                resourceArn=db_cluster_arn,
                transactionId=transaction['transactionId'])
                
        else:
            transaction_response = client.commit_transaction(
                secretArn=db_credentials_secrets_store_arn,
                resourceArn=db_cluster_arn,
                transactionId=transaction['transactionId'])
            print(transaction['transactionId'])
        
        print(f'Transaction Status: {transaction_response["transactionStatus"]}')
        
