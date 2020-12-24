using Confluent.Kafka;
using Domain.AppSettings;
using Domain.Kafka;
using Domain.Models;
using Microsoft.Extensions.Configuration;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Service.Payment
{
    class Program
    {
        static async Task Main(string[] args)
        {
            ConsumeResult<Null, string> subResult;
            DeliveryResult<Null, string> pubResult;
            string error;

            var configuration = new ConfigurationBuilder()
                    .AddJsonFile("appSettings.json", optional: true, reloadOnChange: true).Build();

            var producerProcessedTopicName = AppSettings.GetTopicName(configuration, "Processed");
            var producerReportedTopicName = AppSettings.GetTopicName(configuration, "Reported");

            var kafkaService = new KafkaService(configuration);

            while (true)
            {
                (subResult, error) = kafkaService.Subscribe();

                if (error == string.Empty)
                {
                    var order = JsonSerializer.Deserialize<Order>(subResult.Message.Value);

                    var (report, isProcessed) = DoPaymentProcess(order);

                    string jsonData = JsonSerializer.Serialize(report);
                    (pubResult, error) = await kafkaService.Publish(producerReportedTopicName, jsonData);

                    if (isProcessed)
                        (pubResult, error) = await kafkaService.Publish(producerProcessedTopicName, subResult.Message.Value);
                }
            }
        }

        private static (Report, bool) DoPaymentProcess(Order order)
        {
            var isProcessed = false;

            Thread.Sleep(10000);

            var report = new Report()
            {
                Id = Guid.NewGuid(),
                Order = order,
                CreatedOn = DateTime.Now
            };

            if (order.Price > 50)
            {
                report.Details = "Order has NOT been processed due to failed payment.";
                report.Status = Status.PaymentFailed;
            }
            else
            {
                report.Details = "Order has been processed.";
                report.Status = Status.PaymentProcessed;

                isProcessed = true;
            }

            return (report, isProcessed);
        }
    }
}
