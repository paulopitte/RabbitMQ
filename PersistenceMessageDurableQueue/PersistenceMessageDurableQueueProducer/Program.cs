using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace PersistenceMessageDurableQueueProducer
{

    internal sealed class Program
    {
        /*
         */



        static void Main(string[] args)
        {
            Console.WriteLine("RabbitMQ - Persistence Message e Durable Queue !");
            Console.WriteLine("Publicando fila no Rabbit!");

            var factory = new ConnectionFactory()
            {
                UserName = "guest",
                Password = "guest",
                HostName = "127.0.0.1",
                Port = 5672
            };

            using (var conn = factory.CreateConnection())
            using (var channel = conn.CreateModel())
            {
                var queueName = "test_time_to_live";




                channel.QueueDeclare(queue: queueName,
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var body = Encoding.UTF8.GetBytes($" Notificação gerada em: {DateTime.UtcNow.AddHours(-3)}");
 
                channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: null, body: body);

                Console.WriteLine(" Precione enter para continuar. ");
                Console.ReadLine();
            }

        }

    }
}
