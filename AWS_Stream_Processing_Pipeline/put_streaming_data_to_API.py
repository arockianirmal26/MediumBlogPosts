import pandas as pd
import requests

# write all the rows from local .csv file to the api as Put request
def myfunction():
    myURL = "https://msnznjy6tj.execute-api.us-east-1.amazonaws.com/Production/00-api-resource"

    # Read the file
    data = pd.read_csv('/home/nirmal/Desktop/Andreas_DataEng/creditCardTransactions/data/Transaction_c1.csv', sep=',')

    for i in data.index:
        # print(i)
        try:
            # convert the row to json
            export = data.loc[i].to_json()

            # send it to the api
            response = requests.post(myURL, data=export)

            # print the returncode
            print(export)
            print(response)
        except:
            print(data.loc[i])

if __name__ == "__main__":
    myfunction()
