using Domain.AppSettings;
using Domain.Models;
using Domain.Kafka;
using Microsoft.Extensions.Configuration;
using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Service.Ordering
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appSettings.json", optional: true, reloadOnChange: true).Build();

            var producerSubmittedTopicName = AppSettings.GetTopicName(configuration, "Submitted");
            var producerReportedTopicName = AppSettings.GetTopicName(configuration, "Reported");

            var KafkaService = new KafkaService(configuration);

            while (true)
            {
                var (order, report) = DoOrdering();

                string jsonData = JsonSerializer.Serialize(report);
                var (result, error) = await KafkaService.Publish(producerReportedTopicName, jsonData);

                jsonData = JsonSerializer.Serialize(order);
                (result, error) = await KafkaService.Publish(producerSubmittedTopicName, jsonData);
            }
        }

        private static (Order, Report) DoOrdering()
        {
            var rnd = new Random();

            Thread.Sleep(1000);

            var pId = rnd.Next(111111, 999999);

            var order = new Order()
            {
                Id = Guid.NewGuid(),
                ProductId = pId,
                ProductName = $"Product {pId}",
                Quantity = rnd.Next(1, 10),
                Price = rnd.Next(1, 100),
                CreatedOn = DateTime.Now
            };

            var report = new Report()
            {
                Id = Guid.NewGuid(),
                Order = order,
                Details = "Order has been submitted.",
                Status = Status.OrderSubmitted,
                CreatedOn = DateTime.Now
            };

            return (order, report);
        }
    }
}
