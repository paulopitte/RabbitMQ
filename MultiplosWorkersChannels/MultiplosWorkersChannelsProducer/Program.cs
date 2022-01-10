using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static System.Console;
namespace MultiplosWorkersChannelsProducer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("RabbitMQ C# .Net Core Multiplos Workers e Channels!");
            Console.WriteLine("Populando fila no Rabbit!");


            try
            {


                var factory = new ConnectionFactory()
                {
                    UserName = "guest",
                    Password = "guest",
                    HostName = "127.0.0.1",
                    Port = 5672
                };
                using (var connection = factory.CreateConnection())
                {

                    // Poderiamos aplicar o Pattner AbstractFactory
                    var queueName = "orderQueue";
                    var channel1 = CreateChannel(connection);
                    var channel2 = CreateChannel(connection);
                    var channel3 = CreateChannel(connection);

                    BuildPublishers(channel1, queueName, "Produtor A");
                    BuildPublishers(channel2, queueName, "Produtor B");
                    BuildPublishers(channel3, queueName, "Produtor C");

                    ReadLine();
                }

            }
            catch (System.Exception)
            {

            }
        }

        private static IModel CreateChannel(IConnection connection) =>
             connection.CreateModel();



        private static void BuildPublishers(IModel channel, string queueName, string publishName)
        {

            Task.Run(() =>
            {
                int count = 0;

                channel.QueueDeclare(queue: queueName,
                                         durable: false,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);
                while (true)
                {

                    string message = $"Mensagem: {publishName} : Enviando {count++}";
                    var body = Encoding.UTF8.GetBytes(message);

                    channel.BasicPublish(exchange: "",
                                         routingKey: queueName,
                                         basicProperties: null,
                                         body: body);


                    WriteLine($" [x]: {message}");
                    Thread.Sleep(2000);
                }
            });
        }
    }
}
