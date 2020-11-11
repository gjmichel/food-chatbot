# food-chatbot

This repo contains some of the code used to build a chatbot accessible [here](http://food-chatbot.s3-website-us-east-1.amazonaws.com/).

It has a simple frontend (one web page) stored on S3, which allows the user to interact with the system and enter the information needed for his request. 
The backend consists of a NoSQL database containing several thousand restaurants scrapped from the Yelp API, as well as an ElasticSearch cluster, which allows to identify restaurants meeting the user's criteria (type of food, price, number of guests...) within a radius of 1 mile. The diagram below summarizes the structure of the web application.
Periodically, the Yelp API is re-scrapped to include potential new restaurants. 


Powered by Amazon Lex and specifically trained to interact with the user about restaurants, it understands the user's requests and asks for the additional information required to deliver the best possible recommendations.  These advices are based on several features, including :
  * type of cuisine (Chinese, French, Italian, Korean, Brazilian...)
  * price (on a scale from 1 to 4)
  * user location (Times Square, NYSE, 110th Street & Broadway...)
Concerning the geographical aspect, the user enters the location around which he wants to have lunch or dinner by indicating a neighborhood (ex: Tribeca, Hell's Kitchen...), a recognizable place (ex: Union Square, Flatiron Building...) or a crossroads (ex: 110th Street & Broadway...), which is converted into geographical coordinates within the lambda function, to be taken into account by the Geoquery feature of Elasticsearch. 

Once the necessary information has been submitted, the user is asked for an email address to which he or she will receive the characteristics of two restaurants that match the provided criteria. 

 
 

Apart from some configurations that can be done within the console (create a SQS queue, attach policies to Lambda functions...), it should allow to build the whole backend.


The following figure explains the microservices architecure of the application: 

![Chatbot Structure](https://github.com/gjmichel/food-chatbot/blob/main/Chatbot_structure_.jpg)

