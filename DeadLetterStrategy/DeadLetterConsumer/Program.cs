using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DeadLetterConsumer
{
    internal sealed class Program
    {

        /*
         * A Idéia é interromper o message Loop, ou seja uma mensagem em processamento caso ocorra algum problema na entidade ou atributo, etc..
         * podemos remover a fila de processamento e move-la para uma fila de erros na qual iremos tratar posteriormente.
         * 
         * podemos realizar uma contagem, tipo tente processar 5x caso nao tenha exito entao movemos para fila de erros.
         * 
         */


        private const string DEADLETTER_EXCHANGE = "DeadLetterExchange";
        private const string DEADLETTER_QUEUE = "DeadLetterQueue";
        static async Task Main(string[] args)
        {
            Console.WriteLine("RabbitMQ - Dead Letter Strategy !");
            Console.WriteLine("Consumindo fila no Rabbit!");

            var factory = new ConnectionFactory()
            {
                UserName = "guest",
                Password = "guest",
                HostName = "127.0.0.1",
                Port = 5672
            };

            using (var conn = factory.CreateConnection())
            using (var channel = conn.CreateModel())
            {
                // 1 Passo -  Definição da estrategia 
                // Caso dê algum problema então devolvo a mensagem para fila atraves do argumento setado e referenciado no QueueDeclare para fila task_order
                // então quando ocorrer um erro da mensagem consumida da fila task_order
                channel.ExchangeDeclare(DEADLETTER_EXCHANGE, ExchangeType.Fanout);
                channel.QueueDeclare(queue: DEADLETTER_QUEUE, durable: true, exclusive: false, autoDelete: false, arguments: null);
                channel.QueueBind(DEADLETTER_QUEUE, DEADLETTER_EXCHANGE, routingKey: "");

                var arguments = new Dictionary<string, object>()
                {
                    { "x-dead-letter-exchange", DEADLETTER_EXCHANGE }
                };

                channel.QueueDeclare("task_order", true, false, false, arguments);

                channel.BasicQos(0, 1, false);
                Console.WriteLine("[*] Aguardando mensagens. ");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (s, e) =>
                {
                    try
                    {
                        var messageBody = e.Body.ToArray();
                        var message = Encoding.UTF8.GetString(messageBody);
                        Console.WriteLine(" [*] Mensagem Recebida => {0}", message);


                        var total = int.Parse(message);

                        // caso dê tudo certo até aqui, então removo da fila
                        channel.BasicAck(e.DeliveryTag, false);


                    }
                    catch (Exception ex)
                    {

                        Console.WriteLine($"OPS!!! =>  {ex.Message}");

                        // 2 passo -  Alteração é não podemos devolver para fila de produção, então set o parametro "requeue" para "false"
                        channel.BasicNack(e.DeliveryTag, false, requeue: false);
                    }
                };

                channel.BasicConsume("task_order", false, consumer);


                Console.WriteLine(" Precione enter para continuar. ");
                Console.ReadLine();
            }

        }

    }
}
