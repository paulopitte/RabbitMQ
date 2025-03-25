using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace RabbitMQWorkerConsumer
{
    using Domain;

    internal sealed class Program
    {



        static void Main(string[] args)
        {



            var factory = new ConnectionFactory()
            {
                UserName = "guest",
                Password = "guest",
                HostName = "127.0.0.1",
                Port = 5672
            };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "orderQueue",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    try
                    {
                       var body = ea.Body;
                        var message = Encoding.UTF8.GetString(body);
                        var order = System.Text.Json.JsonSerializer.Deserialize<Order>(message);

                        Console.WriteLine($"Order: {order.Id}|{order.ItemName}|{order.Price:N2}");

                        channel.BasicAck(ea.DeliveryTag, false);
                    }
                    catch (Exception ex)
                    {
                        //Logger
                        channel.BasicNack(ea.DeliveryTag, false, requeue: true);
                    }
                };
                channel.BasicConsume(queue: "orderQueue",
                                     autoAck: false,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }
    }
}
