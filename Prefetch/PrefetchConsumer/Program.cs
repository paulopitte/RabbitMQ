using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PrefetchConsumer
{
    internal sealed class Program
    {


        static async Task Main(string[] args)
        {
            Console.WriteLine("RabbitMQ - Gerenciamento do Buffer de Consumo de Mensagens!");
            Console.WriteLine("Consumindo fila no Rabbit!");



            var factory = new ConnectionFactory()
            {
                UserName = "guest",
                Password = "guest",
                HostName = "127.0.0.1",
                Port = 5672
            };

            using (var connection = factory.CreateConnection())
            {
                var queueName = "orderQueue";
                var channel = await CreateChannel(connection);
                var channel1 = await CreateChannel(connection);

                // para publicador nao podemos reaproveitar channels, a melhor prática será criara um channel exclusivo.
                // para consumidor podemos reaproveitar channel, ou criar um channel para grupos de consumudores
                channel.QueueDeclare(queue: queueName,
                                     durable: false,
                                     exclusive: false,
                                     autoDelete: false,
                                     arguments: null);

                channel1.QueueDeclare(queue: queueName,
                                    durable: false,
                                    exclusive: false,
                                    autoDelete: false,
                                    arguments: null);


                //for (int workIndex = 0; workIndex < 7; workIndex++)
                //    BuildAndRunWorker(channel, $"Worker {workIndex}:{channelIndex}", queueName);

                BuildAndRunWorker(channel, "Worker 1", queueName);
                BuildAndRunWorker(channel1, "Worker 2", queueName);
                BuildAndRunWorker(channel, "Worker 3", queueName);
                BuildAndRunWorker(channel1, "Worker 4", queueName);



                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        private static async ValueTask<IModel> CreateChannel(IConnection connection) =>
             connection.CreateModel();

        private static void BuildAndRunWorker(IModel channel, string workerName, string queueName)
        {

            // PREFECH Qos
            // A Ideia é limitar a quantidade de mensagem obtida para o buffer para um canal.
            // prefetchCount => Limita a pegar 3 mensagem por vez.
            // global => false prefetchCount por consumer | true  prefetchCount por channel 
            channel.BasicQos(prefetchSize: 0, prefetchCount: 3, global: false);






            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                try
                {

                    //AQUI TEMOS O MOMENTO DA APLICAÇÃO DE REGRAS DE NEGOCIO.


                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine($"ChannelNumber: {channel.ChannelNumber} - {workerName} |  [x] Recebendo Notificação {message}");



                    //throw new Exception("ERRO DE REGRA DE NEGOCIO");

                    //A CONFIRMAÇÃO MANUAL É FEITA PELO CHANNEL
                    // confirma a remoção da mensagem 1 a 1 utilizando o false.
                    channel.BasicAck(ea.DeliveryTag, false);
                }
                catch (Exception ex)
                {


                    // UMA MANEIRA DE DEIXAR RESILIENTE NOSSAS APP.
                    channel.BasicNack(ea.DeliveryTag, false, true);


                    // LOG ETC...
                    Console.WriteLine(ex.Message);
                }
            };

            // CONFIRMAÇÃO AUTOMÁTICA QUANDO TRUE,
            // PROBLEMA QUE, QUANDO OCORRER QUALQUER PROBLEMA COM AUTOACK=TRUE IREMOS PERDER AS MENSAGEM DA FILA... POIS JÁ ESTÃO EM BUFFER NA APP.
            // O IDEIAL É SEMPRE CONFIRMAR MANUALMENTE A REMOÇÃO DA MENSAGEM NA FILA.
            channel.BasicConsume(queue: queueName,
                                     autoAck: false,
                                     consumer: consumer);




        }
    }
}
