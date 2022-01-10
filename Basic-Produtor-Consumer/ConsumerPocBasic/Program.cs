using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;

namespace ConsumerPocBasic
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var factory = new ConnectionFactory() { HostName = "localhost" };
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.QueueDeclare(queue: "pocBasic",
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                var consumer = new EventingBasicConsumer(channel);
               
                    consumer.Received += (model, ea) =>
                    {
                        try
                        {

                            var body = ea.Body.ToArray();
                            var message = Encoding.UTF8.GetString(body);
                            Console.WriteLine(" [x] Mensagem Recebida {0}", message);
                            channel.BasicAck(ea.DeliveryTag, true);

                        }
                        catch (Exception e)
                        {
                            channel.BasicAck(ea.DeliveryTag, false);
                        }

                    };
              

                channel.BasicConsume(queue: "pocBasic",
                                     autoAck: false,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                
                Console.ReadLine();
            }
        }
    }
}
