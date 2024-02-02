import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "***REMOVED***"
        self.USER = '***REMOVED***'
        self.PASSWORD = '***REMOVED***'
        self.DATABASE = '***REMOVED***'
        self.PORT = ***REMOVED***
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM ***REMOVED*** LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            pin_stream_name = "streaming-12471ce1b695-pin"
            geo_stream_name = "streaming-12471ce1b695-geo"
            user_stream_name = "streaming-12471ce1b695-user"
            
            pin_payload = json.dumps({
                "StreamName": pin_stream_name,
                "Data": {"index": pin_result["index"], "unique_id": pin_result["unique_id"], 
                         "title": pin_result["title"], "description": pin_result["description"], 
                         "poster_name": pin_result["poster_name"], "follower_count": pin_result["follower_count"], 
                         "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"], 
                         "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], 
                         "save_location": pin_result["save_location"], "category": pin_result["category"]
                        },
                "PartitionKey": "pin"
                    })
            
            geo_payload = json.dumps({
                "StreamName": geo_stream_name,
                "Data": {"index": geo_result["ind"], "timestamp": geo_result["timestamp"].isoformat(), 
                         "latitude": geo_result["latitude"], "longitude": geo_result["longitude"], 
                         "country": geo_result["country"]
                        },
                "PartitionKey": "geo"
                    })
            
            user_payload = json.dumps({
                "StreamName": user_stream_name,
                "Data": {"index": user_result["ind"], "date_joined": user_result["date_joined"].isoformat(), 
                         "first_name": user_result["first_name"], "last_name": user_result["last_name"], 
                         "age": user_result["age"]
                        },
                "PartitionKey": "user"
                    })
                 
            invoke_url = "https://dytgh5kfhl.execute-api.us-east-1.amazonaws.com/test/streams/"
            headers = {'Content-Type': 'application/json'}
            
            pin_stream = requests.request("PUT", f"{invoke_url}{pin_stream_name}/record", headers=headers, data=pin_payload)
            geo_stream = requests.request("PUT", f"{invoke_url}{geo_stream_name}/record", headers=headers, data=geo_payload)
            user_stream = requests.request("PUT", f"{invoke_url}{user_stream_name}/record", headers=headers, data=user_payload)

            print(f"Pin status code = {pin_stream.status_code}")
            print(f"Geo status code = {geo_stream.status_code}")
            print(f"User status code = {user_stream.status_code}")

if __name__ == "__main__":
    run_infinite_post_data_loop()