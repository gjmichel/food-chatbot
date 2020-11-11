
# This function requires a layer with the following libraries : pandas, numpy, requests, datetime, elasticsearch, requests_aws4auth
# -

import json
import pandas as pd
import numpy as np
from datetime import date

import requests
from requests_aws4auth import AWS4Auth

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from elasticsearch import Elasticsearch, RequestsHttpConnection


# YELP API CREDENTIALS
Client_ID = 'your_yelp_API_credentials'
API_Key= 'your_yelp_API_credentials'

# API constants 
API_HOST = 'https://api.yelp.com' #The API url header
SEARCH_PATH = '/v3/businesses/search' #The path for an API request to find businesses
BUSINESS_PATH = '/v3/businesses/'  # The path to get data for a single business

dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')
dynamodb_ressource = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb_ressource.Table('YelpRestaurants')

es_client = boto3.client('es', aws_access_key_id = Access_Key_ID , aws_secret_access_key = Secret_Access_Key , region_name='us-east-1')

credentials = boto3.Session(region_name='us-east-1').get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region ='us-east-1', 'es')





def get_restaurants(API_Key,term, location,number=50):

    #First we get the access token
    #Set up the search data dictionary. Look for other arguments in the API documentation
    search_data = {
    'term': term.replace(' ', '+'),
    'location': location.replace(' ', '+'), # location to search next to, replace the white spaces 
    'limit': number, # number of results we want to get
    'radius': 1000}

    url = API_HOST + SEARCH_PATH
    headers = {'Authorization': 'Bearer %s' % API_Key,} # found in documentation
    response = requests.request('GET', url, headers=headers, params=search_data).json()
    businesses = response.get('businesses')
    return businesses



def get_relevant_info(restaurants): 
  
  df = pd.DataFrame({'Name': [],'ID': [],'CuisineType1':[],'CuisineType2':[], 'Phone':[], 'Address': [], 'ZipCode':[] , 'Coordinates': [], 'Rating': [], 'NumberofReviews':[], 'Website': [], 'PriceLevel': [] })

  for restaurant in restaurants :
      Name = restaurant['name']
      ID = restaurant['id']
      CuisineType1 = restaurant['categories'][0]['title']
      try:
        CuisineType2 = restaurant['categories'][1]['title']
      except:
        CuisineType2 = np.NaN
      Phone = restaurant['phone']
      Address = restaurant['location']['display_address'][0]
      ZipCode = restaurant['location']['zip_code']
      Coordinates = restaurant['coordinates']
      Rating = restaurant['rating']
      N_reviews = int(restaurant['review_count'])
      try:
        Website = restaurant['url']
      except:
        Website = np.NaN
      try:
        PriceLevel = restaurant['price']
      except:
        PriceLevel = 'Unknown'
      df = df.append({'Name':Name, 'ID':ID, 'CuisineType1':CuisineType1, 'CuisineType2':CuisineType2, 'Phone':Phone, 'Address': Address, 'ZipCode':ZipCode , 'Coordinates':Coordinates, 'Rating':Rating, 'NumberofReviews':N_reviews, 'Website':Website, 'PriceLevel':PriceLevel },ignore_index=True)
  return df



def create_table(Table_Name,Primary_Key, Sort_Key):

  try: 
      response = dynamodb_client.create_table(
          TableName = Table_Name, 
          # Declare your Primary Key in the KeySchema argument
          KeySchema=[{     
                        "AttributeName": Primary_Key,
                        "KeyType": "HASH"},  # Primary Key 
                     {
                        "AttributeName": Sort_Key,
                        "KeyType": "RANGE"}],  # Sort Key

          
          # Any attributes used in KeySchema or Indexes must be declared in AttributeDefinitions
          AttributeDefinitions=[{
                                  "AttributeName": Primary_Key,
                                  "AttributeType": "S"},
                                {
                                  "AttributeName": Sort_Key,
                                  "AttributeType": "S"}],
                    
        # ProvisionedThroughput controls the amount of data you can read or write to DynamoDB per second.
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 100})
      print("Table created successfully!")
  except Exception as e:
    print("Error creating table:")
    print(e)





def get_IDs_list():
  response = table.scan()
  items = response['Items']
  IDs_list = []
  for item in items : 
    IDs_list.append(item['ID'])
  return IDs_list






def send_restaurants_to_table(restaurants_json, IDs_list): 
  count = 0
  try: 
    for restaurant in restaurants_json: 
        #print(restaurant['name'])
        Name = restaurant['name']
        ID = restaurant['id']
        if ID not in IDS_list:
          
          try:
            CuisineType1 = restaurant['categories'][0]['title']
          except:
            CuisineType1 = "0"
          
          try:
            CuisineType2 = restaurant['categories'][1]['title']
          except:
            CuisineType2 = "0"

          try:
            CuisineType3 = restaurant['categories'][2]['title']
          except:
            CuisineType3 = "0"

          try:
            Phone = restaurant['phone']
          except:
            Phone = "0"
          
          Address = restaurant['location']['display_address'][0]
          ZipCode = str(restaurant['location']['zip_code'])
          
          latitude = str(restaurant['coordinates']["latitude"]) #D
          longitude = str(restaurant['coordinates']["longitude"]) #D
          Coordinates = {"latitude": latitude, "longitude": longitude}

          try:
            N_reviews = restaurant['review_count'] #I
          except:
            N_reviews = "Unknown"

          try:
            Rating = str(restaurant['rating']) #D
          except:
            Rating = "0"
          
          try:
            Website = restaurant['url']
          except:
            Website = "0"
          
          try:
            PriceLevel = restaurant['price']
          except:
            PriceLevel = "0"

          today = date.today()

          resto = {'Name':Name, 'ID':ID, 'CuisineType1':CuisineType1, 'CuisineType2':CuisineType2,'CuisineType3':CuisineType3, 'Phone':Phone, 'Address': Address, 'ZipCode':ZipCode , 'Coordinates':Coordinates, 'Rating':Rating, 'NumberofReviews':N_reviews, 'Website':Website, 'PriceLevel':PriceLevel, 'insertedAtDate': str(today) }
          

          table.put_item(Item = resto)
          IDS_list.append(ID)
          count +=1
  except : 
    print('No restaurants')
  return count





def scrapping_to_dynamo_db(Type, location, IDs_list): 

  restaurants_json = get_restaurants(API_Key,Type, location,number=50)
  # Send to the database
  count = send_restaurants_to_table(restaurants_json, IDs_list)
  return count



Locations = ['New York Stock Exchange','Pace University','Little Italy', 'Chinatown', 'Lower West Side', 'Tribeca', 'SOHO', 'NOHO', 'Tompkins Square Park', \
             'Lower East Side', 'Lower West Side', 'East Village', 'West Village', 'Greenwich Village', 'Abingdon Square Park', 'Stuyvesant Square', \
             'Union Square', 'Gramercy', 'Chelsea', 'Flatiron Building', 'Madison Square Garden', 'Empire State Building', 'Grand Central Terminal', \
             'Bryant Park', 'Times Square', 'Wall Street',"Hell's Kitchen", 'Midtown', 'Midtown East', 'Turtle Bay', 'Rockefeller Center', 'Lenox Hill', \
             'Colombus Circle', 'Lincoln Square', 'Upper East Side', 'Guggenheim Museum', 'Hayden Planetarium', 'Upper West Side', 'Stephen Gaynor School', \
             'Frederick Douglas Houses', 'Columbia University', 'Morningside Park', 'Riverside Park', 'Marcus Garvey Park', 'Harlem', 'East Harlem', \
             'Gutenberg Playground', 'Sutton Place', 'Kips Bay', 'Pier 40']

Cuisines = ['Breakfast & Brunch', 'Sandwiches', 'Italian', 'Pizza', 'Japanese', 'American',  'Mexican', 'Coffee & Tea', 'Cafes','Sushi Bars', 'Salad', \
            'Chinese','Burgers', 'Seafood', 'Mediterranean', 'Fast Food', 'Wine Bars', 'Indian', 'Korean', 'French', 'Asian Fusion','Vegetarian', 'Halal', \
            'Thai', 'Middle Eastern', 'Noodles', 'Food Trucks', 'Vegan', 'Steakhouses', 'Ramen', 'Barbeque', 'Chicken Wings', 'Bagels', 'Latin American', \
            'Spanish', 'Greek', 'Kosher', 'Vietnamese', 'Tacos', 'Tapas Bars', 'Irish', 'Chicken Shop', 'Caribbean', 'Comfort Food', 'Dim Sum', 'Hot Dogs', \
            'Turkish', 'Poke', 'Cuban','Falafel','Pasta Shops', 'Southern', 'Hookah Bars', 'Pakistani', 'Wraps', 'Creperies', 'Waffles', 'Cantonese', \
            'Izakaya', 'Modern European', 'Japanese Curry']



def scrap_all(Cuisines, Locations): 
  IDs_list = get_IDs_list()
  count = 0
  for cuisine in Cuisines : 
    for location in Locations :
      location_ready = location + 'New York, NY'
      print('Location:', location, 'Cuisine: ', cuisine)
      count += scrapping_to_dynamo_db(cuisine, location_ready, IDs_list)
  return count



def connectES():
    host = 'your_es_cluster'
    region = 'us-east-1'
    service = 'es'

    credentials = boto3.Session(region_name=region).get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es')


    try:
        esClient = Elasticsearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth = awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection)


        return esClient

    except Exception as E:
        print("Unable to connect to {0}".format(esEndPoint))
        print(E)


def send_item_to_es(data, print_status = False): 
    es = connectES()
    es.index(index="restaurants", body=data)
    if print_status == True: 
        print('Data sent to ElasticSearch')


def is_here(ID):
    es = connectES()
    response = es.search(index = 'restaurants',
                         body = {"query": {"bool": {"must": {"match": {"id": ID}}}}}) 
    
    item_in_es = response['hits']['total']['value']
    return item_in_es

def get_coordinates(response): 
    latitude = float(response['Coordinates']['latitude'])
    longitude = float(response['Coordinates']['longitude'])
    dico = {"lat": latitude,
            "lon": longitude}
    return dico
  

def send_to_ElasticSearch(data, print_status = False):
  
  ID = data['id']
  if is_here(ID) == 0 :
    send_item_to_es(data, print_status = False)
    if print_status == True: 
      print('Element sent to ES')

    else: 
      if print_status == True: 
        print('Element was already stored in ES')


def fill_elasticsearch(): 

    relevant_keys_1 = ['ID', 'Name', 'Address', 'CuisineType1', 'CuisineType2', 'CuisineType3', 'PriceLevel', 'Rating']
    relevant_keys_2 = ['ID', 'Name', 'Address', 'CuisineType1', 'CuisineType2', 'PriceLevel', 'Rating']
    relevant_keys_3 = ['ID', 'Name', 'Address', 'CuisineType1', 'PriceLevel','Rating']
    relevant_keys_4 = ['ID', 'Name', 'Address', 'CuisineType1']


    # get data from DynamoDB
    all_restaurants = table.scan()['Items']

    for item in all_restaurants: 
      if 'Coordinates' not in item.keys():
        pass
      else:
        try:
            relevant_data = {k:item[k] for k in relevant_keys_1}
            
            relevant_data['cuisinetype1'] = relevant_data.pop('CuisineType1')
            relevant_data['cuisinetype2'] = relevant_data.pop('CuisineType2')
            relevant_data['cuisinetype3'] = relevant_data.pop('CuisineType3')
            relevant_data['name'] = relevant_data.pop('Name')
            relevant_data['id'] = relevant_data.pop('ID')

            relevant_data['location'] = get_coordinates(item)
            relevant_data['rating']= float(item['Rating'])
            relevant_data['pricelevel'] = float(len(item['PriceLevel']))
          
        except:
            try: 
                relevant_data = {k:item[k] for k in relevant_keys_2}

                relevant_data['cuisinetype1'] = relevant_data.pop('CuisineType1')
                relevant_data['cuisinetype2'] = relevant_data.pop('CuisineType2')
                relevant_data['name'] = relevant_data.pop('Name')
                relevant_data['id'] = relevant_data.pop('ID')

                relevant_data['location'] = get_coordinates(item)
                relevant_data['rating']= float(item['Rating'])
                relevant_data['pricelevel'] = float(len(item['PriceLevel']))
               
            except: 
                try: 
                  relevant_data = {k:item[k] for k in relevant_keys_3}

                  relevant_data['cuisinetype1'] = relevant_data.pop('CuisineType1')
                  relevant_data['name'] = relevant_data.pop('Name')
                  relevant_data['id'] = relevant_data.pop('ID')

                  relevant_data['location'] = get_coordinates(item)
                  relevant_data['rating']= float(item['Rating'])
                  relevant_data['pricelevel'] = float(len(item['PriceLevel']))
                  

                except:
                  relevant_data = {k:item[k] for k in relevant_keys_4}

                  relevant_data['cuisinetype1'] = relevant_data.pop('CuisineType1')

                  relevant_data['location'] = get_coordinates(item)
                  relevant_data['rating']= float(0)
                  relevant_data['pricelevel'] = float(3)
                 
                
        try: 
          send_to_ElasticSearch(relevant_data)
        except :
          print(relevant_data['id'])






def lambda_handler(event, context):

  count = scrap_all(Cuisines, Locations)
  fill_elasticsearch()

  return { 'statusCode': 200,
        'body': json.dumps(str(count))}  






 



