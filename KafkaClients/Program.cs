using Confluent.Kafka;
using System;
using System.Threading;
using System.Configuration;

namespace KafkaClients
{
    class Program
    {
        private static int NumOfMessages = 10;

        public static void Main(string[] args)
        {
            string broker = ConfigurationManager.AppSettings["EH_FQDN"];
            string connectionString = ConfigurationManager.AppSettings["EH_CONNECTION_STRING"];
            string topic = ConfigurationManager.AppSettings["EH_NAME"];
            string consumerGroup = ConfigurationManager.AppSettings["CONSUMER_GROUP"];

            Console.WriteLine("Initializing Producer");
            RunProducer(broker, connectionString, topic);
            Console.WriteLine();
            Console.WriteLine("Initializing Consumer");
            RunConsumer(broker, connectionString, consumerGroup, topic);
            Console.ReadKey();

        }

        public static void RunProducer(string broker, string connectionString, string topic)
        {
            // Set producer config
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = broker,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = connectionString,
            };

            // Key is set as Null since we are not using it
            using (var p = new ProducerBuilder<Null, string>(producerConfig).Build())
            {
                try
                {
                    // Sending fixed number of messages using Produce method to process 
                    // many messages in rapid succession instead of using ProduceAsync
                   for (int i=0; i < NumOfMessages; i++)
                   {

                        string value = "message-" + i;
                        Console.WriteLine($"Sending message with key: not-specified," +
                            $"value: {value}, partition-id: not-specified");
                        p.Produce(topic, new Message<Null, string> { Value = value });
                   }

                   // Wait up to 10 seconds for any inflight messages to be sent
                    p.Flush(TimeSpan.FromSeconds(10));
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed with error: {e.Error.Reason}");
                }
            }
        }

        // To simulate multiple consumers, strip out this code in its own program
        // and run both programs simultaneously
        public static void RunConsumer(string broker, string connectionString, string consumerGroup, string topic)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = broker,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SocketTimeoutMs = 60000,
                SessionTimeoutMs = 30000,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "$ConnectionString",
                SaslPassword = connectionString,
                GroupId = consumerGroup,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var c = new ConsumerBuilder<string, string>(consumerConfig).Build())
            {
                c.Subscribe(topic);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var message = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed - key: {message.Message.Key}, "+
                                $"value: {message.Message.Value}, " +
                                $"partition-id: {message.Partition}," +
                                $"offset: {message.Offset}");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch(OperationCanceledException)
                {
                    // This ensures the consumer leaves the group cleanly 
                    // and final offsets are committed
                    c.Close();
                }
            }
        }
    }
}
