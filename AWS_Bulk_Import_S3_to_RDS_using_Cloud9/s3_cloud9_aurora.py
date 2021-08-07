import boto3 as bt3
import time
from datetime import datetime
import json

s3 = bt3.client('s3') # connection to s3
csv1_data = s3.get_object(Bucket='00-s3-transaction', Key='Transaction_c1.csv') # read the csv file
contents_1 = csv1_data['Body'].read().split(b'\n')

for row in contents_1[1:]:  #avoid the header row
    my_list = str(row)
    my_list = my_list.split(',')
    print('--------Actual Row--------------')
    print(my_list)
    if (my_list[0]!="b''"): # skipping the last empty row
        print('--------Separate Elements-------------')
        #print ('details:', my_list[0][2:])  # id		
        id1 = my_list[0][2:]
        print (id1)
        #print ('details:', my_list[1][1:-1]) # trans_date_trans_time
        trans_date_trans_time = my_list[1][1:-1]
        print (trans_date_trans_time)
        #print ('details:', my_list[2][1:-1]) # cc_num
        cc_num = my_list[2][1:-1]
        if (cc_num==''):
            cc_num = None
        print (cc_num)
        #print ('details:', my_list[3][1:-1]) # merchant
        merchant = my_list[3][1:-1]
        print (merchant)
        #print ('details:', my_list[4][1:-1]) # category
        category = my_list[4][1:-1]
        print (category)
        #print ('details:', my_list[5]) # amt
        amt = my_list[5]
        print (amt)
        #print ('details:', my_list[6][1:-1]) # first
        first = my_list[6][1:-1]
        print (first)
        #print ('details:', my_list[7][1:-1]) # last
        last = my_list[7][1:-1]
        print (last)
        #print ('details:', my_list[8][1:-1]) # gender
        gender = my_list[8][1:-1]
        print (gender)
        #print ('details:', my_list[9][1:-1]) # street
        street = my_list[9][1:-1]
        print (street)
        #print ('details:', my_list[10][1:-1]) # city
        city = my_list[10][1:-1]
        print (city)
        #print ('details:', my_list[11][1:-1]) # state
        state = my_list[11][1:-1]
        print (state)
        #print ('details:', my_list[12]) # zip
        zip1 = my_list[12]
        print (zip1)
        #print ('details:', my_list[13]) # lat
        lat = my_list[13]
        print (lat)
        #print ('details:', my_list[14]) # long
        long1 = my_list[14]
        print (long1)
        #print ('details:', my_list[15]) # city_pop
        city_pop = my_list[15]
        print (city_pop)
        #print ('details:', my_list[16][1:-1]) # job
        job = my_list[16][1:-1]
        print (job)
        #print ('details:', my_list[17][1:-1]) # dob 
        dob = my_list[17][1:-1]
        print (dob)
        #print ('details:', my_list[18][1:-1]) # trans_num
        trans_num = my_list[18][1:-1]
        print (trans_num)
        if (trans_num==''):
            trans_num = None
        #print ('details:', my_list[19]) # unix_time
        unix_time = my_list[19]
        print (unix_time)
        #print ('details:', my_list[20]) # merch_lat
        merch_lat = my_list[20]
        print (merch_lat)
        #print ('details:', my_list[21]) # merch_long
        merch_long = my_list[21]
        print (merch_long)
        #print ('details:', my_list[22][:-1]) # is_fraud
        is_fraud = my_list[22][:-1]
        print (is_fraud)
        
        
    
        client = bt3.client('rds-data')
        db_cluster_arn = "arn:aws:rds:us-east-1:293324326406:cluster:creditcardtransactionsoltp"
        db_credentials_secrets_store_arn = "arn:aws:secretsmanager:us-east-1:293324326406:secret:rds-db-credentials/cluster-OCAZ5KLOBT3DS2KVADGNNFC57Y/admin-PV6R6U"
        database_name = "TransactionsOLTP"
        
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
            sql ="insert into CITY(CITY_NAME, STATE, CITY_POPULATION) values ('" + city+ "','" + state + "', '" + str(city_pop) + "' ) ON DUPLICATE KEY UPDATE CITY_NAME = VALUES(CITY_NAME), STATE = VALUES(STATE), CITY_POPULATION = VALUES(CITY_POPULATION)"
            response = execute_statement( sql,transaction['transactionId'] )
            print(transaction['transactionId'])
            
        
            #------------insert/update into 'ADDRESS' table 
            sql ="insert into ADDRESS(STREET, ZIP, LAT, LONGI) values ('" + street + "','" + zip1 + "', '" + str(lat) + "' , '" + str(long1) + "') ON DUPLICATE KEY UPDATE STREET = VALUES(STREET), ZIP = VALUES(ZIP), LAT = VALUES(LAT) , LONGI = VALUES(LONGI)"
            response = execute_statement( sql,transaction['transactionId'] )
            
        
            sql ="update ADDRESS a inner join CITY c set a.CITY_ID = c.CITY_ID where a.STREET='" + street + "' and a.ZIP=" + str(zip1) + " and c.CITY_NAME='"+ city + "' and c.STATE='"+ state + "'"
            response = execute_statement( sql,transaction['transactionId'] )
            
            #------------insert/update into 'CUSTOMER' table 

            sql ="insert into CUSTOMER(FIRST_NAME, LAST_NAME, CREDIT_CARD_NUMBER, GENDER, JOB, DATE_OF_BIRTH) values ('" + first + "','" + last + "', '" + str(cc_num) + "' , '" + gender + "', '" + job + "', '" + dob + "') ON DUPLICATE KEY UPDATE FIRST_NAME = VALUES(FIRST_NAME), LAST_NAME = VALUES(LAST_NAME), CREDIT_CARD_NUMBER = VALUES(CREDIT_CARD_NUMBER) , GENDER = VALUES(GENDER) , JOB = VALUES(JOB), DATE_OF_BIRTH = VALUES(DATE_OF_BIRTH)"
            response = execute_statement( sql,transaction['transactionId'] )
            
            
            sql ="update CUSTOMER c inner join ADDRESS a set c.ADDR_ID = a.ADDR_ID where c.CREDIT_CARD_NUMBER='" + str(cc_num) + "' and a.STREET='" + street + "' and a.ZIP=" + str(zip1) + ""
            response = execute_statement( sql,transaction['transactionId'] )
           
            #------------insert into 'MERCHANT' table 
            
            sql ="insert into MERCHANT(MERCHANT_NAME, CATEGORY, LAT, LONGI) values ('" + merchant + "','" + category + "', '" + str(merch_lat) + "' , '" + str(merch_long) + "') ON DUPLICATE KEY UPDATE MERCHANT_NAME = VALUES(MERCHANT_NAME), CATEGORY = VALUES(CATEGORY), LAT = VALUES(LAT) , LONGI = VALUES(LONGI)"
            response = execute_statement( sql,transaction['transactionId'] )
            
        
            #------------insert/update into 'TRANSACTION' table 
            
            sql ="insert into TRANSACTION(TRANSACTION_ID, AMOUNT, TRANSFER_TIMESTAMP, UNIX_TIME, IS_FRAUD) values ('" + trans_num + "','" + str(amt) + "','" + str(trans_date_trans_time) + "', '" + str(unix_time) + "' , '" + str(is_fraud) + "') ON DUPLICATE KEY UPDATE TRANSACTION_ID = VALUES(TRANSACTION_ID), AMOUNT = VALUES(AMOUNT), TRANSFER_TIMESTAMP = VALUES(TRANSFER_TIMESTAMP) , UNIX_TIME = VALUES(UNIX_TIME) , IS_FRAUD = VALUES(IS_FRAUD)"
            response = execute_statement( sql,transaction['transactionId'] )
            

            sql ="update TRANSACTION t inner join CUSTOMER c set t.CUST_ID = c.CUST_ID where t.TRANSACTION_ID='" + trans_num + "' and c.CREDIT_CARD_NUMBER='" + str(cc_num) + "'"
            response = execute_statement( sql,transaction['transactionId'] )
            
        
        
            sql ="update TRANSACTION t inner join MERCHANT m set t.MERCHANT_ID = m.MERCHANT_ID where t.TRANSACTION_ID='" + trans_num+ "' and m.MERCHANT_NAME='" + merchant + "' and m.CATEGORY='" + category+ "'"
            response = execute_statement( sql,transaction['transactionId'] )
            
            
        except Exception as e:
            print('row',my_list)
                #validate(instance=p_record, schema=trans_schema)
            print("Given row is InValid")
             # generate the name for the file with the timestamp to write to s3
            dateTimeObj = datetime.now()
            timestampStr = dateTimeObj.strftime("%d-%b-%Y-%H%M%S,%f")
            mykey = 'output-' + timestampStr + str(round(time.time() * 1000)) + '.txt'
            print(mykey)
            #put the file into the s3 bucket
            response = s3.put_object(Body=json.dumps(my_list), Bucket='00-s3-invalid-transactions', Key= mykey)
                
                
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
   
           
        
