using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MultiplosWorkersChannelsConsumer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("RabbitMQ C# .Net Core Multiplos Workers e Channels!");
            Console.WriteLine("Consumindo fila no Rabbit com vários channels!");


            var factory = new ConnectionFactory()
            {
                UserName = "guest",
                Password = "guest",
                HostName = "127.0.0.1",
                Port = 5672
            };
            using (var connection = factory.CreateConnection())
            {

                for (int channelIndex = 0; channelIndex < 2; channelIndex++)
                {
                    var queueName = "orderQueue";
                    var channel = await CreateChannel(connection);

                    // para publicador nao podemos reaproveitar channels, a melhor prática será criara um channel exclusivo.
                    // para consumidor podemos reaproveitar channel, ou criar um channel para grupos de consumudores
                    channel.QueueDeclare(queue: queueName,
                                         durable: false,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);




                    for (int workIndex = 0; workIndex < 7; workIndex++)
                        await BuildAndRunWorker(channel, $"Worker {workIndex}:{channelIndex}");

                    //BuildAndRunWorker(channel, "Worker 1");
                    //BuildAndRunWorker(channel, "Worker 2");
                    //BuildAndRunWorker(channel, "Worker 3");
                    //BuildAndRunWorker(channel, "Worker 4");



                }

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        private static async ValueTask<IModel> CreateChannel(IConnection connection) =>
             connection.CreateModel();

        private static async ValueTask BuildAndRunWorker(IModel channel, string workerName)
        {
            await Task.Run(() =>
             {
                 var consumer = new EventingBasicConsumer(channel);
                 consumer.Received += (model, ea) =>
                 {
                     var body = ea.Body.ToArray();
                     var message = Encoding.UTF8.GetString(body);
                     Console.WriteLine($"{channel.ChannelNumber} - {workerName} |  [x] Recebendo Notificação {message}");
                 };
                 channel.BasicConsume(queue: "orderQueue",
                                      autoAck: true,
                                      consumer: consumer);

             });

           
        }

    }
}
