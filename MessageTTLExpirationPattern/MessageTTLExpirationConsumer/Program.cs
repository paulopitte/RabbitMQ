using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace MessageTTLExpirationConsumer
{

    internal sealed class Program
    {
        /*
         * A idéia é gerar uma mensagem que tenha um tempo de vida de expiração caso não seja consumida dentro do tempo estipulado.
         * a mensagem será excluida da fila automaticamente.         
         *  existe 2 formas de configuração, 
         *  1 - criar um argumento na criação da queue já setando o tipo TTL atraves de header com chave "x-message-ttl"
         *  2 - definir a propriedade na mensagem a ser publicada pelo CreateBasicProperties()
         */



        static void Main(string[] args)
        {
            Console.WriteLine("RabbitMQ - Message TTL Expiration !");
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

                // ** FORMA DE ESPECIFICAÇÃO DE TTL PELO ARQUIMENTO NA DECLARAÇÃO DO QUEUE
                // Uma das formas é criar uma fila com tipo(feature) TTL
                //ou podemos configurar o TTL passando pelas propriedades na publicação da mensagem.
                var argumento = new Dictionary<string, object>
                {
                    { "x-message-ttl", 20000 }
                };


                channel.QueueDeclare(queue: queueName,
                    durable: true, 
                    false, 
                    false, 
                    argumento);

                var body = Encoding.UTF8.GetBytes($" Notificação gerada em: {DateTime.UtcNow.AddHours(-3)}");


                // ** FORMA DE ESPECIFICAÇÃO DE TTL PELA MENSAGEM

                //var prop = channel.CreateBasicProperties();
                //prop.Expiration = "20000"; 
                //channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: prop, body: body);
                
                channel.BasicPublish(exchange: "", routingKey: queueName, basicProperties: null, body: body);

                Console.WriteLine(" Precione enter para continuar. ");
                Console.ReadLine();
            }

        }

    }
}
