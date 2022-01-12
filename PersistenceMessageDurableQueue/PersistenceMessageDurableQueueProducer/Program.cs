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
                var queueName = "helloQueue";

                channel.QueueDeclare(queue: queueName,
                                     durable: true, // esse parametro diz ao Rabbit que a fila não será excluida caso a reinicialização da app ou container, etc...
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var baseProp = channel.CreateBasicProperties();

                // Esta propriedade configura ao Rabbbit em manter a Mensagem persistida mesmo desligando o container ou alguma falha de infra, etc...
                baseProp.Persistent = true; // persiste fisicamente no disco


                string msg = $" Notificação gerada em: {DateTime.UtcNow.AddHours(-3)}";
                var body = Encoding.UTF8.GetBytes(msg);
 


                channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: baseProp, body: body);
                Console.WriteLine(msg);
                Console.WriteLine(" Pressione enter para continuar. ");
                Console.ReadLine();
            }

        }

    }
}
