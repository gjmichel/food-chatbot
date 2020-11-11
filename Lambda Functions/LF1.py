import json
import boto3



# Get the service resource
sqs_client = boto3.client('sqs', region_name='us-east-1')

# We will get the queue using its URL
queue_url = 'https://sqs.us-east-1.amazonaws.com/635132305149/Q1.fifo'



def lambda_handler(event, context):
    
    intent_type = event["currentIntent"]["name"]
    slots = event["currentIntent"]["slots"]
    sessionAttributes = event["sessionAttributes"]
   
    
    if intent_type == "GreetingIntent" : 
        salutation = slots["SayHello"]

        starving = 'hungry'
        
        return {
        "sessionAttributes": sessionAttributes,    
        "dialogAction": {
            "type" : 'Close',
            "fulfillmentState": "Fulfilled",
            "message": {
                'contentType': "PlainText", 
                'content': f"Hey buddy, I knew you were {starving} ! What can I do for you ?"
                }
            }}
            
    elif intent_type == 'ThankyouIntent': 
        nicePhrase = slots["NicePhrase"]
        thanks = slots["thanks"]
        
        return {
        "sessionAttributes": sessionAttributes,    
        "dialogAction": {
            "type" : 'Close',
            "fulfillmentState": "Fulfilled",
            "message": {
                'contentType': "PlainText", 
                'content': f"My pleasure, I'm happy to help!"
                }
            }}
    
    elif intent_type == 'DiningSuggestionIntent':
    
        numGuests = slots["numberofpeople"]
        mealTime = slots["whentoeat"]
        location = slots["wheretoeat"]
        cuisine = slots["whichcuisine"]
        phonenumber = slots["yourphonenumber"] 
        email = slots['email']
        price_level = slots['price_level']
        
        
        Message_Attributes={'Cuisine': {'DataType': 'String','StringValue': str(cuisine)},
                            'PhoneNumber':{'DataType': 'String','StringValue': str(phonenumber)},
                            'PriceLevel': {'DataType': 'String','StringValue': str(price_level)},
                            'Email': {'DataType': 'String','StringValue': str(email)},
                            'Location': {'DataType': 'String','StringValue': str(location)},
                            'Time': {'DataType': 'String','StringValue': str(mealTime)}, 
                            'numGuests': {'DataType': 'String','StringValue': str(numGuests)}}
                            
        string_to_send = "You can find the content of the request in messageAttributes."
                            
                            
        queue_message = sqs_client.send_message(QueueUrl=queue_url,
                                                MessageAttributes=Message_Attributes,
                                                MessageBody= json.dumps(string_to_send),
                                                MessageGroupId= "1")
        
        

        print(queue_message['MessageId'])
        
        
            
        return {
        "sessionAttributes": sessionAttributes,    
        "dialogAction": {
            "type" : 'Close',
            "fulfillmentState": "Fulfilled",
            "message": {
                'contentType': "PlainText", 
                'content': f"Sounds good! I'll give you my suggestions regarding {cuisine} for {numGuests} at {mealTime} next to {location} (budget of {str(price_level)}/3). You'll receive an email at {email} shortly! Have a good one."
                }
            }}


       