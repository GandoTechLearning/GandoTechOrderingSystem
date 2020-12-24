using Confluent.Kafka;
using Domain.Kafka;
using Domain.Models;
using Microsoft.Extensions.Configuration;
using System;
using System.Text.Json;

namespace Service.Reporting
{
    class Program
    {
        static void Main(string[] args)
        {
            var configuration = new ConfigurationBuilder()
                .AddJsonFile("appSettings.json", optional: true, reloadOnChange: true).Build();

            var kafkaService = new KafkaService(configuration);

            while (true)
            {
                ConsumeResult<Null, string> subResult;
                string error;

                (subResult, error) = kafkaService.Subscribe();

                if (error == string.Empty)
                {
                    var report = JsonSerializer.Deserialize<Report>(subResult.Message.Value);
                    Console.WriteLine($"[Report Status: {report.Status}] => [ CreatedOn: {report.CreatedOn}, Report Id: {report.Id}, Order Id: {report.Order.Id} , Report Details: {report.Details} ]");
                }
            }
        }
    }
}
