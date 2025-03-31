using RabbitMQ.Client;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using static System.Console;
namespace AckNackAutoAckProducer;
sealed class Program
{
    static void Main(string[] args)
    {
        WriteLine("RabbitMQ - Ack Nack AutoAck com C# .Net Core Confirmação de Recebimento e o ManualResetEvent para segurar a execução de uma Thread até que receba um evento.!");
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

            using var connection = factory.CreateConnection();

            // TODO: Poderiamos aplicar o Padrão AbstractFactory

            // Atenção: para publicador nao podemos reaproveitar channels, a melhor prática será criara um channel exclusivo.
            // para consumidor podemos reaproveitar channel, ou criar um channel para grupos de consumudores
            var queueName = "orderQueue";
            var channel1 = CreateChannel(connection);

            BuildPublishers(channel1, queueName, "Produtor A", manualResetEvent);

            //Trava a subthread para execução da publicação.
            manualResetEvent.WaitOne();

        }
        catch (System.Exception)
        {

        }
    }
    private static IModel CreateChannel(IConnection connection) => connection.CreateModel();



    private static void BuildPublishers(IModel channel, string queueName, string publishName, ManualResetEvent manualResetEvent)
    {
        Task.Run(() =>
       {
           int menssagemNumber = 0;
           channel.QueueDeclare(queue: queueName,
                               durable: false,
                               exclusive: false,
                               autoDelete: false,
                               arguments: null);
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

                       channel.BasicPublish(exchange: "",
                                            routingKey: queueName,
                                            basicProperties: null,
                                            body: body);


                       WriteLine($" [x]: {message}");
                       Thread.Sleep(200);
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
