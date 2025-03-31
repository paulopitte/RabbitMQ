using RabbitMQ.Client;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Console;
namespace Producer;

/*
 * RabbitMQ Puslish Confirmation e Return Message
 * um otima opção de arquitetura para tratativa de todo fluxo de confirmação de mensagem.
 * 
 * */
sealed class Program
{
    static async Task Main(string[] args)
    {
        Console.WriteLine("RabbitMQ Puslish Confirmation e Return Message!");
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

                // 1 - passo é habilitar ou ativar o processo de confirmação, pois não vem por padrao ativada, tbm pode ser por canal individual.
                channel.ConfirmSelect();

                // 2 - passo: podemos capturar pelos evetos as confirmações ou nao, sendo assim temos a possibilidade de realizar tratativas. 
                channel.BasicNacks += Channel_BasicNacks;
                channel.BasicAcks += Channel_BasicAcks;
                channel.BasicReturn += Channel_BasicReturn;

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

                    channel.BasicPublish(exchange: "", // Exchange  null default
                                         routingKey: "orderQueuexxxxx",
                                         basicProperties: null,
                                         body: body,
                                         mandatory: true);

                    // - Esse cara, habilta a verificação de algo que possa ocorrer no brocker, exemplo: não existencia de uma exchange ou regra de bind,
                    channel.WaitForConfirms(new TimeSpan(0, 0, 5));


                    WriteLine($" [x] Enviando Mensagem: {message}");
                    await Task.Delay(2000);
                }
            }


        }
        catch (System.Exception)
        {

        }
    }


    // Caso seja enviada a mensagem para uma fila invalida, esse evento realiza a captura do returno para tratativa
    // sendo assim temos o total controle do fluxo de publicção de uma mensagem.
    private static void Channel_BasicReturn(object sender, RabbitMQ.Client.Events.BasicReturnEventArgs e)
    {

        var msg = Encoding.UTF8.GetString(e.Body.ToArray());
        WriteLine($"{DateTime.Now.ToString("o")} -> Basic Return  - Mensagem  não processada e devolvida para tratativas - {msg}");

    }

    // Toda vez que tivermos uma negativa de entrega da mensaem teremos um evento sendo executado
    private static void Channel_BasicAcks(object sender, RabbitMQ.Client.Events.BasicAckEventArgs e)
    {
        WriteLine($"{DateTime.Now.ToString("o")} -> Basic Nack  - Mensagem  não processada");
    }

    // Retorno do Brocker informando o recebimento da mensagem com sucesso
    private static void Channel_BasicNacks(object sender, RabbitMQ.Client.Events.BasicNackEventArgs e)
    {
        WriteLine($"{DateTime.Now.ToString("o")} -> Basic Ack - Mensagem recebida ");

    }
}


