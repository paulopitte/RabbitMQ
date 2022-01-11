using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AckNackAutoAckConsumer
{
    internal sealed class Program
    {


        static async Task Main(string[] args)
        {
            Console.WriteLine("RabbitMQ - AutoAck!");
            Console.WriteLine("Consumindo fila no Rabbit!");

            /*
             *
                1) O requeue do Nack não é obrigatório, você pode emitir um Nack com requeue false pro caso de querer descartar a mensagem.

                2) O Nack não é uma implementação nativa do AMQP, ele é uma extensão do RabbitMQ criada em cima do Reject. Ambos tem o mesmo princípio: descartar ou reenfileirar mensagens com erro de processamento. Contudo, o Nack permite fazer isso em batch através da flag multiple, enquanto com o Reject só podemos fazer uma a uma.

                3) No exemplo tenho um consumidor só (uma thread), mas no caso de consumidores concorrentes, o requeue tanto do Nack como do Reject vão tentar colocar a mensagem de volta na fila ou na posição original ou, se não for possível, o mais próximo possível do início da fila. Isso pode variar de acordo com o número de consumidores, com o prefetch (QoS) setado pra cada consumidro e tal.

                4) Nem o Nack nem o Reject impedem que o consumidor pare de receber mensagens. Ele só as descartam/reenfileram. Se isso quiser ser feito, deve ser enviado o comando Cancel para o broker e o consumidor espera um CancelOk do broker pra de fato interromper o consumo.
             * 
             * */


            var factory = new ConnectionFactory()
            {
                UserName = "guest",
                Password = "guest",
                HostName = "127.0.0.1",
                Port = 5672
            };

            using (var connection = factory.CreateConnection())
            {

                for (int channelIndex = 0; channelIndex < 2; channelIndex++)
                {
                    var queueName = "orderQueue";
                    var channel = await CreateChannel(connection);

                    // para publicador nao podemos reaproveitar channels, a melhor prática será criara um channel exclusivo.
                    // para consumidor podemos reaproveitar channel, ou criar um channel para grupos de consumudores
                    channel.QueueDeclare(queue: queueName,
                                         durable: false,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);




                    for (int workIndex = 0; workIndex < 7; workIndex++)
                        BuildAndRunWorker(channel, $"Worker {workIndex}:{channelIndex}", queueName);

                    //BuildAndRunWorker(channel, "Worker 1");
                    //BuildAndRunWorker(channel, "Worker 2");
                    //BuildAndRunWorker(channel, "Worker 3");
                    //BuildAndRunWorker(channel, "Worker 4");

                }

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }
        }

        private static async ValueTask<IModel> CreateChannel(IConnection connection) =>
             connection.CreateModel();

        private static void BuildAndRunWorker(IModel channel, string workerName, string queueName)
        {

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                try
                {

                    //AQUI TEMOS O MOMENTO DA APLICAÇÃO DE REGRAS DE NEGOCIO.


                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine($"{channel.ChannelNumber} - {workerName} |  [x] Recebendo Notificação {message}");



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
