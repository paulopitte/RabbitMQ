using Domain;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace RoundRobinConsumer
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("RabbitMQ RPC - Remote Procedure Call - Client!");
            Console.WriteLine("Consumindo fila no Rabbit!");

            #region Configurações


            // Conexão, channel
            var factory = new ConnectionFactory()
            {
                UserName = "guest",
                Password = "guest",
                HostName = "127.0.0.1",
                Port = 5672
            };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            // Fila de Resposta e TransictionID ou CorrelationID
            var replyQueue = $"{nameof(Order)}_return";
            var correlationId = Guid.NewGuid().ToString();

            channel.QueueDeclare(queue: replyQueue, durable: false, exclusive: false, autoDelete: false, arguments: null);
            channel.QueueDeclare(queue: nameof(Order), durable: false, exclusive: false, autoDelete: false, arguments: null);


            #endregion

            #region Consumidor

            // Consumidor
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                // Aqui podemos implementar qualquer tipo de Estatégia como DeadLetter, Confirmação ou incluir em uma nova fila para posteriormente processa-la .
                if (correlationId == ea.BasicProperties.CorrelationId)
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Mensagem Recedida com CorrelationID correto {0}", message);
                    return;
                }

                Console.WriteLine($"Mensagem descartada, Identificações de Correlação inválidos",
                    $"original {correlationId} recebido {ea.BasicProperties.CorrelationId} ");

            };
            channel.BasicConsume(queue: replyQueue, autoAck: true, consumer: consumer);

            #endregion


            #region     Produtor

            //Produtor
            var Props = channel.CreateBasicProperties();
            Props.CorrelationId = correlationId;
            Props.ReplyTo = replyQueue;



            while (true)
            {
                Console.WriteLine($" Informe o Valor do Pedido: ");
                var ammount = decimal.Parse(s: Console.ReadLine());

                Order order = new(ammount);
                var message = JsonSerializer.Serialize(order);
                var body = Encoding.UTF8.GetBytes(message);

                channel.BasicPublish(exchange: "", routingKey: nameof(Order), basicProperties: Props, body: body);

                Console.WriteLine("Mensagem Publicada....", $"{message}\n\n");
                Console.ReadKey();
                Console.Clear();

            }

            #endregion

        }
    }
}
