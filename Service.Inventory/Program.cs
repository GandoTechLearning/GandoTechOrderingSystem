using Confluent.Kafka;
using Domain.AppSettings;
using Domain.Kafka;
using Domain.Models;
using Microsoft.Extensions.Configuration;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Service.Inventory
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

            var producerValidatedTopicName = AppSettings.GetTopicName(configuration, "Validated");
            var producerReportedTopicName = AppSettings.GetTopicName(configuration, "Reported");

            var kafkaService = new KafkaService(configuration);

            while (true)
            {
                (subResult, error) = kafkaService.Subscribe();

                if (error == string.Empty)
                {
                    var order = JsonSerializer.Deserialize<Order>(subResult.Message.Value);

                    var (report, isValidated) = DoInventory(order);

                    string jsonData = JsonSerializer.Serialize(report);
                    (pubResult, error) = await kafkaService.Publish(producerReportedTopicName, jsonData);

                    if (isValidated)
                        (pubResult, error) = await kafkaService.Publish(producerValidatedTopicName, subResult.Message.Value);
                }
            }
        }

        private static (Report, bool) DoInventory(Order order)
        {
            bool isValidated = false;

            Thread.Sleep(1);

            var report = new Report()
            {
                Id = Guid.NewGuid(),
                Order = order,
                Details = "Order has NOT been validated due to out of stock.",
                Status = Status.OrderOutOfStock,
                CreatedOn = DateTime.Now
            };

            if (order.Quantity < 6)
            {
                report.Details = "Order has been validated.";
                report.Status = Status.OrderValidated;

                isValidated = true;
            }

            return (report, isValidated);
        }
    }
}
