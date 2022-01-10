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
                    int count = 0;
                    // Poderiamos aplicar o Pattner AbstractFactory
                    // para publicador nao podemos reaproveitar channels, a melhor prática será criara um channel exclusivo.
                    // para consumidor podemos reaproveitar channel, ou criar um channel para grupos de consumudores
                    var queueName = "orderQueue";
                    var channel1 = CreateChannel(connection);
                    var channel2 = CreateChannel(connection);
                    var channel3 = CreateChannel(connection);
                    while (true)
                    {

                        await BuildPublishers(channel1, queueName, "Produtor A", count);
                        await BuildPublishers(channel2, queueName, "Produtor B", count);
                        await BuildPublishers(channel3, queueName, "Produtor C", count);
                        count++;
                    }
                    ReadLine();
                }

            }
            catch (System.Exception)
            {

            }
        }

        private static IModel CreateChannel(IConnection connection) =>
             connection.CreateModel();



        private static async Task BuildPublishers(IModel channel, string queueName, string publishName, int menssagemNumber)
        {
            await Task.Run(() =>
            {
                channel.QueueDeclare(queue: queueName,
                                         durable: false,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);


                string message = $"ChannelNumber {channel.ChannelNumber} | PublishName: {publishName} : Mensagem Number: {menssagemNumber++}";
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(exchange: "",
                                     routingKey: queueName,
                                     basicProperties: null,
                                     body: body);


                WriteLine($" [x]: {message}");
                Thread.Sleep(1000);

            });
        }
    }
}
