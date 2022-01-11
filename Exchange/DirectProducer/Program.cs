using RabbitMQ.Client;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using static System.Console;

namespace DirectProducer
{

    internal sealed class Program
    {

        /*
         * RabbitMQ Exchange Direct - Defini uma chave de rota customizada, 
         * 
         * 
         */


        static void Main(string[] args)
        {
            WriteLine("RabbitMQ -  Exchange Direct!");
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
                    var channel2 = SetupChannel(connection);
                    var channel3 = SetupChannel(connection);
                    var channel4 = SetupChannel(connection);
                    var channel5 = SetupChannel(connection);

                    BuildAndRunPublishers(channel1, "Produtor A - canal 1", manualResetEvent);
                    BuildAndRunPublishers(channel2, "Produtor B - canal 2", manualResetEvent);
                    BuildAndRunPublishers(channel3, "Produtor C - canal 3", manualResetEvent);
                    BuildAndRunPublishers(channel4, "Produtor D - canal 4", manualResetEvent);
                    BuildAndRunPublishers(channel5, "Produtor E - canal 5", manualResetEvent);

                    //Trava a subthread para execução da publicação.
                    manualResetEvent.WaitOne();
                }

            }
            catch (System.Exception ex)
            {
                WriteLine(ex.Message);
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
            channel.ExchangeDeclare(exchange: "order", type: "direct");

            // configuramos explicitamente a associação de uma fila estará ligada a um exchange.
            channel.QueueBind(queue: "order", exchange: "order", routingKey: "order_new");
            channel.QueueBind(queue: "logs", exchange: "order", routingKey: "order_upd");
            channel.QueueBind(queue: "finance_orders", exchange: "order", routingKey: "order_new");

            return channel;
        }



        private static void BuildAndRunPublishers(IModel channel, string publishName, ManualResetEvent manualResetEvent)
        {

            Task.Run(() =>
          {
              Parallel.For(0, 100000000, i =>
                     {

                         var randon = new Random(DateTime.UtcNow.Millisecond * DateTime.UtcNow.Second);
                         Order order = new(i, randon.Next(1000, 9999));
                         var message1 = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(order));
                         try
                         {

                             // WriteLine("Pressione qualquer tecla para publicar novas mensagens...");
                             // ReadKey();

                             channel.BasicPublish("order", "order_new", null, message1);
                             WriteLine($"Novo Pedido Recebido {order.Id} || Amount: {order.Amount} || Criado em: {order.CreateDate:o}");

                             order.UpdateOrder(randon.Next(100, 999));
                             var message2 = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(order));

                             channel.BasicPublish("order", "order_upd", null, message2);
                             WriteLine($"Pedido Atualizado {order.Id} || Amount: {order.Amount} || Atualizado em: {order.LastUpdate:o}");

                             message2 = null;
                         }
                         catch (Exception ex)
                         {
                             WriteLine(ex.Message);
                             manualResetEvent.Set();
                         }
                         finally
                         {
                             message1 = null;
                             order = null;
                             randon = null;
                         }
                     });
          });

        }
    }
}
