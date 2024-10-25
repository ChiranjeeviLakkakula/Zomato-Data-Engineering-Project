import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd 

def zomato(zomato_dataset):
    restaurent_list = []
    for zomato in zomato_dataset:
        for restaurents in zomato['restaurants']:
            restaurent_id = restaurents['restaurant']['id']
            restaurent_name = restaurents['restaurant']['name']
            country_code = restaurents['restaurant']['location']['country_id']
            city = restaurents['restaurant']['location']['city']
            address = restaurents['restaurant']['location']['address']
            locality = restaurents['restaurant']['location']['locality']
            locality_verbose = restaurents['restaurant']['location']['locality_verbose']
            latitude = restaurents['restaurant']['location']['latitude']
            longitude = restaurents['restaurant']['location']['longitude']
            cuisines = restaurents['restaurant']['cuisines']
            average_cost_for_two = restaurents['restaurant']['average_cost_for_two']
            currency = restaurents['restaurant']['currency']
            has_table_booking = restaurents['restaurant']['has_table_booking']
            has_online_delivery = restaurents['restaurant']['has_online_delivery']
            is_delivering_now = restaurents['restaurant']['is_delivering_now']
            switch_to_order_menu = restaurents['restaurant']['switch_to_order_menu']
            price_range = restaurents['restaurant']['price_range']
            aggregate_rating = restaurents['restaurant']['user_rating']['aggregate_rating']
            rating_color = restaurents['restaurant']['user_rating']['rating_color']
            rating_text = restaurents['restaurant']['user_rating']['rating_text']
            votes = restaurents['restaurant']['user_rating']['votes']
            url = restaurents['restaurant']['url']
            menu = restaurents['restaurant']['menu_url']
    
            restaurent_details = {'Restaurent ID':restaurent_id,'Restaurent Name':restaurent_name,'Country Code':country_code,'City':city,'Address':address,'Locality':locality,'Locality Verbose':locality_verbose,
                                    'Latitude':latitude,'Longitude':longitude,'Cuisines':cuisines,'Average Cost for two':average_cost_for_two,'Currency':currency,'Has Table booking':has_table_booking,
                                    'Has Online delivery':has_online_delivery,'Is Delivering now':is_delivering_now,'Switch to order menu':switch_to_order_menu,'Price Range':price_range,
                                    'Aggregate Rating':aggregate_rating,'Rating Color':rating_color,'Rating Text':rating_text,'Votes':votes,'URL':url,'Menu':menu}
            
            restaurent_list.append(restaurent_details)
            
    return restaurent_list


def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    Bucket = "zomoto-data-engineering-project"
    Key = "raw_data/to_processed/"
    
    
    zomato_data = []
    zomato_keys = []
    for file in s3.list_objects(Bucket=Bucket, Prefix=Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            response = s3.get_object(Bucket = Bucket, Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            zomato_data.append(jsonObject)
            zomato_keys.append(file_key)
            
    for zomato_dataset in zomato_data:
        restaurent_list = zomato(zomato_dataset)
        
        restaurent_df = pd.DataFrame.from_dict(restaurent_list)
        
        
        zomato_key = "transformed_data/zomato_restaurents/zomato_restaurents_transformed_" + str(datetime.now()) + ".csv"
        zomato_buffer=StringIO()
        restaurent_df.to_csv(zomato_buffer, index=False)
        zomato_content = zomato_buffer.getvalue()
        s3.put_object(Bucket=Bucket, Key=zomato_key, Body=zomato_content)
        
    s3_resource = boto3.resource('s3')
    for key in zomato_keys:
        copy_source = {
            'Bucket': Bucket,
            'Key': key
        }
        s3_resource.meta.client.copy(copy_source, Bucket, 'raw_data/processed/' + key.split("/")[-1])    
        s3_resource.Object(Bucket, key).delete()