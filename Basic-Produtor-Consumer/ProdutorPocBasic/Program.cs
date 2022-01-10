using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading;

namespace ProdutorPocBasic
{
    internal class Program
    {
        static void Main(string[] args)
        {        
            var message = "Basic Poc RabbitMq! ";

            Console.WriteLine(message);

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "pocBasic",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);


                int loop = 1;
                while (true) {

                    var body = Encoding.UTF8.GetBytes(string.Concat(message, " - ", loop));

                    channel.BasicPublish(exchange: "",
                                         routingKey: "pocBasic",
                                         basicProperties: null,
                                         body: body);
                    Console.WriteLine(" [x] Mensagem Enviada {0} => {1}", message, loop);
                    loop++;
                    Thread.Sleep(100);
                }
            }

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}
