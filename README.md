# food-chatbot

This repo contains some of the code used to build a chatbot accessible [here](http://food-chatbot.s3-website-us-east-1.amazonaws.com/).

Powered by Amazon Lex and ElasticSearch, it recommends restaurants in manhattan based on several criteria, including :
  * type of cuisine (Chinese, French, Italian, Korean, Brazilian...)
  * price (on a scale from 1 to 4)
  * user location (Times Square, NYSE, 110th Street & Broadway...)

Apart from some configurations that can be done within the console (create a SQS queue, attach policies to Lambda functions...), it should allow to build the whole backend.


The following figure explains the microservices architecure of the application: 

![Chatbot Structure](https://github.com/gjmichel/food-chatbot/blob/main/Chatbot_structure_.jpg)

