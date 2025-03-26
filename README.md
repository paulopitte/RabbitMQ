# RabbitMQ
API Rest e Worker com RabbitMQ C#  Net Core Producer e Consumer.


 docker run -d --hostname rabbitserver --name rabbit-server -p 15672:15672 -p 5672:5672 rabbitmq:3-management


 # latest RabbitMQ 4.0.x
docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.0-management