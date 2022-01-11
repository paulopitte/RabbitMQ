using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static System.Console;
namespace FanoutProducer
{
    class Program
    {
        static void Main(string[] args)
        {
            WriteLine("RabbitMQ -  Exchange Fanout - cópias massivas de mensagens para mais de uma fila!");
            WriteLine("Populando fila no Rabbit!");


            try
            {


                ConnectionFactory factory = new()
                {
                    UserName = "guest",
                    Password = "guest",
                    HostName = "127.0.0.1",
                    Port = 5672
                };




                //Ideia é travar a thread e caso dê algum problema podemos destravar a execução 
                // em mode ditático usamos o console.ReadLine() para travar a execução,
                // forma correta é usar o ManualResetEvent
                ManualResetEvent manualResetEvent = new(false);
                manualResetEvent.Reset();


                using (var connection = factory.CreateConnection())
                {


                    // TODO: Poderiamos aplicar o Pattner AbstractFactory

                    // Atenção: para publicador nao podemos reaproveitar channels, a melhor prática será criara um channel exclusivo.
                    // para consumidor SIM podemos reaproveitar channel, ou criar um channel para grupos de consumudores
                  
                    var channel1 = SetupChannel(connection);

                    BuildAndRunPublishers(channel1, "Produtor A", manualResetEvent);

                    //Trava a subthread para execução da publicação.
                    manualResetEvent.WaitOne();
                }

            }
            catch (System.Exception)
            {

            }
        }
        private static IModel SetupChannel(IConnection connection)
        {
            var channel = connection.CreateModel();

            // configura as filas
            channel.QueueDeclare(queue: "order", durable: false, exclusive: false, autoDelete: false, arguments: null);
            channel.QueueDeclare(queue: "logs", durable: false, exclusive: false, autoDelete: false, arguments: null);
            channel.QueueDeclare(queue: "finance_orders", durable: false, exclusive: false, autoDelete: false, arguments: null);


            // configura o nome e tipo de exchange.
            channel.ExchangeDeclare(exchange: "order", type: "fanout");

            // configuramos explicitamente a associação de uma fila estará ligada a um exchange.
            channel.QueueBind(queue: "order", exchange: "order", routingKey: "");
            channel.QueueBind(queue: "logs", exchange: "order", routingKey: "");
            channel.QueueBind(queue: "finance_orders", exchange: "order", routingKey: "");

            return channel;
        }



        private static void BuildAndRunPublishers(IModel channel, string publishName, ManualResetEvent manualResetEvent)
        {
            Task.Run(() =>
            {
                int menssagemNumber = 0;

                while (true)
                {

                    WriteLine("Pressione qualquer tecla para publicar 10 mensagens");
                    ReadKey();

                    try
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            string message = $"Mensagem(Count loop) Number {menssagemNumber++} | from: {publishName} ";
                            var body = Encoding.UTF8.GetBytes(message);

                            channel.BasicPublish(exchange: "order",
                                                 routingKey: "",
                                                 basicProperties: null,
                                                 body: body);


                            WriteLine($"publishName: {publishName} -  [x]: {message}");
                            Thread.Sleep(10);
                        }
                    }
                    catch (Exception ex)
                    {
                        WriteLine(ex.Message);

                        //Libera a thread para proximo processamento.
                        manualResetEvent.Set();
                    }
                }
            });

        }
    }
}
