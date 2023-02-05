import json
import urllib.parse
import awswrangler as wr
#import boto3
import os
import pandas as pd

#OS/path Settings

output_path =os.environ['output_path']
database_name=os.environ['database_name']
table_name=os.environ['table_name']
data_write_operation=os.environ['data_write_operation']



def lambda_handler(event, context):
    

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    
    try:
        
        #Creating df
        df_raw = wr.s3.read_json('s3://{}/{}'.format(bucket,key))
        
        #Extracting items
        df_items = pd.json_normalize(df_raw['items'])
        
        
        #Write to S3
        write = wr.s3.to_parquet(
            df = df_items,
            path = output_path,
            dataset=True,
            database = database_name,
            table = table_name,
            mode =data_write_operation
        
            )
        
        
        
        
        return write
        
            
         
        
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
              
