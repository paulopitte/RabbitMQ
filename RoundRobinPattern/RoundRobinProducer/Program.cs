using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading.Tasks;
using static System.Console;
namespace RoundRobinProducer
{

    /*
     * RabbitMQ Round Robin Pattern com C# .Net Core Balanceamento de Carga com Multiplos Workers
     * 
     * */
    sealed class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("RabbitMQ Round Robin Pattern com C# .Net Core Balanceamento de Carga com Multiplos Workers!");
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
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queue: "orderQueue",
                                         durable: false,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);
                    int count = 0;
                    while (true)
                    {

                        string message = $"OrderNumber: {count++}";
                        var body = Encoding.UTF8.GetBytes(message);

                        channel.BasicPublish(exchange: "",
                                             routingKey: "orderQueue",
                                             basicProperties: null,
                                             body: body);


                        WriteLine($" [x] Enviando Mensagem: {message}");
                        await Task.Delay(100);
                    }
                }


            }
            catch (System.Exception)
            {

            }
        }
    }
}
