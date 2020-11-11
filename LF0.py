
import json
import boto3

lex_client = boto3.client('lex-runtime')


def lambda_handler(event, context):
    
   
    user_input = json.loads(event['body'])
    user_message = user_input['messages'][0]['unstructured']['text']
    response = lex_client.post_text(
                            botName='OrderFoodbot',
                            botAlias='Test',
                            userId='1234',
                            inputText=user_message)

    lex_text_response = response['message']

    
    response_body = {
                'messages': [ {
                'type': "unstructured", 
                'unstructured': {'text': lex_text_response } 
                } 
                ]
            }
    
    return { 
        'statusCode': 200,
        'headers': {   'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(response_body)
    }  



