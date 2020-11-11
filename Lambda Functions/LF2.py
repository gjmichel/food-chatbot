
# this function requires custom layers containing the elasticsearch, requests_aws4auth and geopy libraries

import json
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from geopy.geocoders import Nominatim




dynamodb_ressource = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb_ressource.Table('YelpRestaurants')
ses_client = boto3.client('ses', region_name='us-east-1')
credentials = boto3.Session(region_name='us-east-1').get_credentials()




def connectES():
    
    host = 'your_es_cluster_endpoint'
    region = 'us-east-1'
    service = 'es'

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




def find_restaurant_on_ES(cuisine_type, user_location, min_rating =0, price_level = 2):

    es = connectES()
    
    user_location_ = user_location + ", NY, NY"
    geolocator = Nominatim(user_agent = "agent")
    location = geolocator.geocode(user_location_)
    user_latitude, user_longitude = location.latitude, location.longitude    
    
    response = es.search(index = 'restaurants',
                        body = {"query": {
                                        "bool": {
                                            "must": [{
                                                  "bool": {
                                                      "should": [{"match": {"cuisinetype1": cuisine_type}},
                                                                 {"match": {"cuisinetype2": cuisine_type}},
                                                                 {"match": {"cuisinetype3": cuisine_type}}]}},
                                              {
                                                  "bool": {
                                                      "must": {"range": {"PriceLevel": {"lte" :1 + price_level}}}}},
                                              {                                                
                                                  "bool": {
                                                      "must": {"range": {"Rating": {"gte" : min_rating }}}}},
                                              {                                                
                                                  "bool": {
                                                      "filter": {"geo_distance": {"distance": "1km",
                                                                                 "location": {"lat": user_latitude, "lon": user_longitude} }}}}] }}},
    
                           size = 2)
                                              
    if len(response['hits']['hits'])!=2:
        response = es.search(index = 'restaurants',
                        body = {"query": {
                                        "bool": {
                                            "must": [{
                                                  "bool": {
                                                      "should": [{"match": {"cuisinetype1": cuisine_type}},
                                                                 {"match": {"cuisinetype2": cuisine_type}},
                                                                 {"match": {"cuisinetype3": cuisine_type}}]}},
                                              {
                                                  "bool": {
                                                      "must": {"range": {"PriceLevel": {"lte" :1 + price_level}}}}},
                                              {                                                
                                                  "bool": {
                                                      "must": {"range": {"Rating": {"gte" : min_rating }}}}},
                                              {                                                
                                                  "bool": {
                                                      "filter": {"geo_distance": {"distance": "2km",
                                                                                 "location": {"lat": user_latitude, "lon": user_longitude} }}}}] }}},
    
                           size = 2)
    
                         
    if len(response['hits']['hits'])!=2:
        response = es.search(index = 'restaurants',
                        body = {"query": {
                                        "bool": {
                                            "must": [{
                                                  "bool": {
                                                      "should": [{"match": {"cuisinetype1": cuisine_type}},
                                                                 {"match": {"cuisinetype2": cuisine_type}},
                                                                 {"match": {"cuisinetype3": cuisine_type}}]}},
                                              {
                                                  "bool": {
                                                      "must": {"range": {"PriceLevel": {"lte" :4}}}}},
                                              {                                                
                                                  "bool": {
                                                      "must": {"range": {"Rating": {"gte" : min_rating }}}}},
                                              {                                                
                                                  "bool": {
                                                      "filter": {"geo_distance": {"distance": "3km",
                                                                                 "location": {"lat": user_latitude, "lon": user_longitude} }}}}] }}},
    
                           size = 2)
        
    restaurants = response['hits']['hits']
    
    ids_list = []
    cuisine_type1_list = []
    
    for restaurant in restaurants: 
        ids_list.append(restaurant['_source']['id'])
        cuisine_type1_list.append(restaurant['_source']['cuisinetype1'])
    
    return ids_list,cuisine_type1_list


def query_restaurant_in_dynamo(cuisine_type, user_location, price_level):

    ES_response_ids, ES_response_keys = find_restaurant_on_ES(cuisine_type,user_location,price_level)
    dynamo_response = []
    for restaurant_id, restaurant_type in zip(ES_response_ids,ES_response_keys):
        dynamo_response.append(table.get_item(Key={"CuisineType1": restaurant_type, "ID": restaurant_id})['Item'])

    return dynamo_response


def process_response(dynamo_response): 
    
    
    
    Adress_1 = dynamo_response[0]['Address']
    Name_1 = dynamo_response[0]['Name']
    Website_1 = dynamo_response[0]['Website']
    Price_1 = dynamo_response[0]['PriceLevel']
    Rating_1 = dynamo_response[0]['Rating']
    Phone_1 = dynamo_response[0]['Phone']
    
    string_1 = f"My first suggestion is {Name_1} at {Adress_1}. " \
             f"The reviews are good ({Rating_1}) and the price is correct ({Price_1}) " \
             f"You can call them at {Phone_1}. "
    
    try: 
        Adress_2 = dynamo_response[1]['Address']
        Name_2 = dynamo_response[1]['Name']
        Website_2 = dynamo_response[1]['Website']
        Price_2 = dynamo_response[1]['PriceLevel']
        Rating_2 = dynamo_response[1]['Rating']
        Phone_2 = dynamo_response[1]['Phone']
    
        string_2 = f"If you're not conviced about this one, please check this other possibility: {Name_2} at {Adress_2}. " \
             f"The reviews ({Rating_2}) and the price level ({Price_2}) match your requirements too! " \
             f"You can call them at {Phone_2}"
    except : 
        string_2 = "I did not find other restaurants in this area matching your requirements. " \
                   "Please submit another search request if the first recommendation does not satisfy your requirements."
    
    return string_1, string_2




def get_html_body(string_to_insert): 

    Intital_body ="""
  <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
  <html xmlns="http://www.w3.org/1999/xhtml" xmlns:o="urn:schemas-microsoft-com:office:office" style="width:100%;font-family:helvetica, 'helvetica neue', arial, verdana, sans-serif;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;padding:0;Margin:0">
   <head> 
    <meta charset="UTF-8"> 
    <meta content="width=device-width, initial-scale=1" name="viewport"> 
    <meta name="x-apple-disable-message-reformatting"> 
    <meta http-equiv="X-UA-Compatible" content="IE=edge"> 
    <meta content="telephone=no" name="format-detection"> 
    <title>Nouveau modèle de courrier électronique 2020-10-29</title> 
    <!--[if (mso 16)]>
      <style type="text/css">
      a {text-decoration: none;}
      </style>
      <![endif]--> 
    <!--[if gte mso 9]><style>sup { font-size: 100% !important; }</style><![endif]--> 
    <!--[if gte mso 9]>
  <xml>
      <o:OfficeDocumentSettings>
      <o:AllowPNG></o:AllowPNG>
      <o:PixelsPerInch>96</o:PixelsPerInch>
      </o:OfficeDocumentSettings>
  </xml>
  <![endif]--> 
    <!--[if !mso]><!-- --> 
    <link href="https://fonts.googleapis.com/css?family=Lato:400,400i,700,700i" rel="stylesheet"> 
    <!--<![endif]--> 
    <style type="text/css">
  #outlook a {
    padding:0;
  }
  .ExternalClass {
    width:100%;
  }
  .ExternalClass,
  .ExternalClass p,
  .ExternalClass span,
  .ExternalClass font,
  .ExternalClass td,
  .ExternalClass div {
    line-height:100%;
  }
  .es-button {
    mso-style-priority:100!important;
    text-decoration:none!important;
  }
  a[x-apple-data-detectors] {
    color:inherit!important;
    text-decoration:none!important;
    font-size:inherit!important;
    font-family:inherit!important;
    font-weight:inherit!important;
    line-height:inherit!important;
  }
  .es-desk-hidden {
    display:none;
    float:left;
    overflow:hidden;
    width:0;
    max-height:0;
    line-height:0;
    mso-hide:all;
  }
  @media only screen and (max-width:800px) {p, ul li, ol li, a { font-size:17px!important; line-height:150%!important } h1 { font-size:30px!important; text-align:center; line-height:120%!important } h2 { font-size:26px!important; text-align:left; line-height:120%!important } h3 { font-size:20px!important; text-align:left; line-height:120%!important } h1 a { font-size:30px!important; text-align:center } h2 a { font-size:20px!important; text-align:left } h3 a { font-size:20px!important; text-align:left } .es-menu td a { font-size:16px!important } .es-header-body p, .es-header-body ul li, .es-header-body ol li, .es-header-body a { font-size:16px!important } .es-footer-body p, .es-footer-body ul li, .es-footer-body ol li, .es-footer-body a { font-size:17px!important } .es-infoblock p, .es-infoblock ul li, .es-infoblock ol li, .es-infoblock a { font-size:12px!important } *[class="gmail-fix"] { display:none!important } .es-m-txt-c, .es-m-txt-c h1, .es-m-txt-c h2, .es-m-txt-c h3 { text-align:center!important } .es-m-txt-r, .es-m-txt-r h1, .es-m-txt-r h2, .es-m-txt-r h3 { text-align:right!important } .es-m-txt-l, .es-m-txt-l h1, .es-m-txt-l h2, .es-m-txt-l h3 { text-align:left!important } .es-m-txt-r img, .es-m-txt-c img, .es-m-txt-l img { display:inline!important } .es-button-border { display:inline-block!important } a.es-button { font-size:14px!important; display:inline-block!important; border-width:15px 25px 15px 25px!important } .es-btn-fw { border-width:10px 0px!important; text-align:center!important } .es-adaptive table, .es-btn-fw, .es-btn-fw-brdr, .es-left, .es-right { width:100%!important } .es-content table, .es-header table, .es-footer table, .es-content, .es-footer, .es-header { width:100%!important; max-width:600px!important } .es-adapt-td { display:block!important; width:100%!important } .adapt-img { width:100%!important; height:auto!important } .es-m-p0 { padding:0px!important } .es-m-p0r { padding-right:0px!important } .es-m-p0l { padding-left:0px!important } .es-m-p0t { padding-top:0px!important } .es-m-p0b { padding-bottom:0!important } .es-m-p20b { padding-bottom:20px!important } .es-mobile-hidden, .es-hidden { display:none!important } tr.es-desk-hidden, td.es-desk-hidden, table.es-desk-hidden { width:auto!important; overflow:visible!important; float:none!important; max-height:inherit!important; line-height:inherit!important } tr.es-desk-hidden { display:table-row!important } table.es-desk-hidden { display:table!important } td.es-desk-menu-hidden { display:table-cell!important } .es-menu td { width:1%!important } table.es-table-not-adapt, .esd-block-html table { width:auto!important } table.es-social { display:inline-block!important } table.es-social td { display:inline-block!important } }
  </style> 
   </head> 
   <body style="width:100%;font-family:helvetica, 'helvetica neue', arial, verdana, sans-serif;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;padding:0;Margin:0"> 
    <div class="es-wrapper-color" style="background-color:#F1F1F1"> 
     <!--[if gte mso 9]>
        <v:background xmlns:v="urn:schemas-microsoft-com:vml" fill="t">
          <v:fill type="tile" color="#f1f1f1"></v:fill>
        </v:background>
      <![endif]--> 
     <table class="es-wrapper" width="100%" cellspacing="0" cellpadding="0" style="mso-table-lspace:0pt;mso-table-rspace:0pt;border-collapse:collapse;border-spacing:0px;padding:0;Margin:0;width:100%;height:100%;background-repeat:repeat;background-position:center top"> 
       <tr style="border-collapse:collapse"> 
        <td valign="top" style="padding:0;Margin:0"> 
         <table class="es-content" cellspacing="0" cellpadding="0" align="center" style="mso-table-lspace:0pt;mso-table-rspace:0pt;border-collapse:collapse;border-spacing:0px;table-layout:fixed !important;width:100%"> 
           <tr style="border-collapse:collapse"> 
            <td align="center" style="padding:0;Margin:0"> 
             <table class="es-content-body" style="mso-table-lspace:0pt;mso-table-rspace:0pt;border-collapse:collapse;border-spacing:0px;background-color:#FFFFFF;border-top:1px solid #DDDDDD;border-right:1px solid #DDDDDD;border-left:1px solid #DDDDDD;width:600px;border-bottom:1px solid #DDDDDD" cellspacing="0" cellpadding="0" bgcolor="#ffffff" align="center"> 
               <tr style="border-collapse:collapse"> 
                <td align="left" style="padding:0;Margin:0;padding-top:40px;padding-left:40px;padding-right:40px"> 
                 <table width="100%" cellspacing="0" cellpadding="0" style="mso-table-lspace:0pt;mso-table-rspace:0pt;border-collapse:collapse;border-spacing:0px"> 
                   <tr style="border-collapse:collapse"> 
                    <td valign="top" align="center" style="padding:0;Margin:0;width:518px"> 
                     <table width="100%" cellspacing="0" cellpadding="0" role="presentation" style="mso-table-lspace:0pt;mso-table-rspace:0pt;border-collapse:collapse;border-spacing:0px"> 
                       <tr style="border-collapse:collapse"> 
                        <td align="center" style="padding:0;Margin:0;padding-top:5px;padding-bottom:15px"><h2 style="Margin:0;line-height:24px;mso-line-height-rule:exactly;font-family:lato, 'helvetica neue', helvetica, arial, sans-serif;font-size:20px;font-style:normal;font-weight:bold;color:#333333">LOOK AT WHAT I FOUND FOR YOU !</h2></td> 
                       </tr> 
                       <tr style="border-collapse:collapse"> 
                        <td align="left" style="padding:0;Margin:0;padding-bottom:10px"><p style="Margin:0;-webkit-text-size-adjust:none;-ms-text-size-adjust:none;mso-line-height-rule:exactly;font-size:15px;font-family:helvetica, 'helvetica neue', arial, verdana, sans-serif;line-height:23px;color:#555555"><strong>After some research, these are the two top restaurants around you that match your requirements !&nbsp;</strong><br></p></td> 
                       </tr> 
                       <tr style="border-collapse:collapse"> 
                        <td align="left" style="padding:0;Margin:0;padding-top:10px;padding-bottom:10px"><p style="Margin:0;-webkit-text-size-adjust:none;-ms-text-size-adjust:none;mso-line-height-rule:exactly;font-size:15px;font-family:helvetica, 'helvetica neue', arial, verdana, sans-serif;line-height:23px;color:#555555">Enter Recommendations Here.</p></td> 
                       </tr> 
                       <tr style="border-collapse:collapse"> 
                        <td align="left" style="padding:0;Margin:0;padding-top:10px;padding-bottom:10px"><p style="Margin:0;-webkit-text-size-adjust:none;-ms-text-size-adjust:none;mso-line-height-rule:exactly;font-size:15px;font-family:helvetica, 'helvetica neue', arial, verdana, sans-serif;line-height:23px;color:#555555">Enjoy your meal and please let me know if you need additional recommendations,</p></td> 
                       </tr> 
                     </table></td> 
                   </tr> 
                 </table></td> 
               </tr> 
               <tr style="border-collapse:collapse"> 
                <td align="left" style="Margin:0;padding-top:10px;padding-bottom:40px;padding-left:40px;padding-right:40px"> 
                 <!--[if mso]><table style="width:518px" cellpadding="0"
                              cellspacing="0"><tr><td style="width:39px" valign="top"><![endif]--> 
                 <table class="es-left" cellspacing="0" cellpadding="0" align="left" style="mso-table-lspace:0pt;mso-table-rspace:0pt;border-collapse:collapse;border-spacing:0px;float:left"> 
                   <tr style="border-collapse:collapse"> 
                    <td class="es-m-p0r es-m-p20b" valign="top" align="center" style="padding:0;Margin:0;width:39px"> 
                     <table width="100%" cellspacing="0" cellpadding="0" role="presentation" style="mso-table-lspace:0pt;mso-table-rspace:0pt;border-collapse:collapse;border-spacing:0px"> 
                       <tr style="border-collapse:collapse"> 
                        <td align="left" style="padding:0;Margin:0;font-size:0"><img src="https://storage.googleapis.com/gjmbucket1/Capture%20d%E2%80%99e%CC%81cran%202020-10-29%20a%CC%80%2016.30.03.png" alt style="display:block;border:0;outline:none;text-decoration:none;-ms-interpolation-mode:bicubic" width="45"></td> 
                       </tr> 
                     </table></td> 
                   </tr> 
                 </table> 
                 <!--[if mso]></td><td style="width:20px"></td><td style="width:459px" valign="top"><![endif]--> 
                 <table cellspacing="0" cellpadding="0" align="right" style="mso-table-lspace:0pt;mso-table-rspace:0pt;border-collapse:collapse;border-spacing:0px"> 
                   <tr style="border-collapse:collapse"> 
                    <td align="left" style="padding:0;Margin:0;width:459px"> 
                     <table width="100%" cellspacing="0" cellpadding="0" role="presentation" style="mso-table-lspace:0pt;mso-table-rspace:0pt;border-collapse:collapse;border-spacing:0px"> 
                       <tr style="border-collapse:collapse"> 
                        <td align="left" style="padding:0;Margin:0;padding-top:10px"><p style="Margin:0;-webkit-text-size-adjust:none;-ms-text-size-adjust:none;mso-line-height-rule:exactly;font-size:14px;font-family:helvetica, 'helvetica neue', arial, verdana, sans-serif;line-height:21px;color:#222222"><strong>Guillaume Michel</strong></p></td> 
                       </tr> 
                       <tr style="border-collapse:collapse"> 
                        <td align="left" style="padding:0;Margin:0"><p style="Margin:0;-webkit-text-size-adjust:none;-ms-text-size-adjust:none;mso-line-height-rule:exactly;font-size:14px;font-family:helvetica, 'helvetica neue', arial, verdana, sans-serif;line-height:21px;color:#666666">Graduate Student | Columbia University</p></td> 
                       </tr> 
                     </table></td> 
                   </tr> 
                 </table> 
                 <!--[if mso]></td></tr></table><![endif]--></td> 
               </tr> 
             </table></td> 
           </tr> 
         </table></td> 
       </tr> 
     </table> 
    </div>  
   </body>
  </html>
  """  
  elements = Intital_body.split("Enter Recommendations Here")
  new_body = elements[0] + string_to_insert + elements[1] 
  return new_body




def send_email(email_sender, email_recipient, recomendations): 

  subject = 'Restaurant recommendations :)'
  charset = "UTF-8"

  BODY_HTML = get_html_body(recomendations)
  
  try: 
    response = ses_client.send_email(
        Destination = {'ToAddresses': [email_recipient]},
        Message = {'Body': {
                      'Html': {'Charset': charset,'Data': BODY_HTML,}},
                  'Subject': {'Charset': charset,'Data': subject}},
        Source = email_sender)
    
  except ClientError as e:
    print(e.response['Error']['Message'])
  else:
    print("Email sent!")




def lambda_handler(event,context):
  
  
    
    cuisine_type = event['Records'][0]['messageAttributes']['Cuisine']['stringValue']
    location = event['Records'][0]['messageAttributes']['Location']['stringValue']
    price_level = int(event['Records'][0]['messageAttributes']['PriceLevel']['stringValue'])
    email = str(event['Records'][0]['messageAttributes']['Email']['stringValue'])
    

    try:
        #get restaurant data from Dynamo for restaurants returned by ES
        recommended_restaurants = query_restaurant_in_dynamo(cuisine_type,location,price_level= price_level)
        sentence_1, sentence_2 = process_response(recommended_restaurants)
    except:
        sentence_1 = "Something went wrong. I did not find any restaurant matching your requirements"
        sentence_2 = "Please submit another request, I will be happy to process it !"
    
    sentence_to_insert = str(sentence_1 + str("<br>") + str("<br>") + sentence_2)
    
      # send email
    send_email('guillaume.playground@gmail.com', email, sentence_to_insert)
    
  
    return {
        'status_code': 200,
        'body': json.dumps(sentence_to_insert)}

