using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Demo.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Serializers = Kafka.Demo.Models.Serializers;

namespace Kafka.Demo.Producer
{
	public class MagicProducer : BackgroundService
	{
		private readonly ProducerConfig ProducerConfig;
		private readonly ILogger Logger;

		public MagicProducer(IOptionsSnapshot<ProducerConfig> producerConfig, ILogger<MagicProducer> logger)
		{
			ProducerConfig = producerConfig.Value;
			Logger = logger;
		}

		protected override Task ExecuteAsync(CancellationToken stoppingToken)
		{
			StartProducingMessages(stoppingToken);
			return Task.CompletedTask;
		}

		private async Task StartProducingMessages(CancellationToken stoppingToken)
		{
			try
			{
				Logger.LogInformation("Starting to produce messages.");
				await ProduceMessages(stoppingToken);
				Logger.LogInformation("Stopped producing messages.");
			}
			catch (Exception e)
			{
				Logger.LogError("Uncaught exception. {Message}", e.Message);
			}
		}

		private async Task ProduceMessages(CancellationToken stoppingToken)
		{
			var topicPartition = new TopicPartition("test_topic", 0);
			var producerBuilder = new ProducerBuilder<string, TestObject>(ProducerConfig);
			producerBuilder.SetKeySerializer(new Serializers.JsonSerializer());
			producerBuilder.SetValueSerializer(new Serializers.JsonSerializer());
			using (var producer = producerBuilder.Build())
			{
				while (!stoppingToken.IsCancellationRequested)
				{
					var testObject = GetTestObject();
					var message = new Message<string, TestObject>
					{
						Headers = null,
						Key = testObject.IdNumber,
						Timestamp = new Timestamp(DateTime.Now),
						Value = testObject
					};

					var slowProduce = await producer.ProduceAsync(topicPartition.Topic, message);
					producer.Produce(topicPartition.Topic, message, HandleResult); //Fast Produce (Let Kafka Partition)
					producer.Produce(topicPartition, message, HandleResult); // Fast Produce (Manually Specify Partition)
					Logger.LogInformation("Successfull delivery. {Key}:{Status}", slowProduce.Message.Key, slowProduce.Value.Status);
				}
				Logger.LogInformation("Producer cancelled.");
			}
		}

		private void HandleResult(DeliveryReport<string, TestObject> dr)
		{
			if (dr.Error.Code != ErrorCode.NoError)
			{
				Logger.LogError("Failed delivery. {Code}:{Message}", dr.Error.Code, dr.Message);
			}
			else
			{
				Logger.LogInformation("Successfull delivery: {Key}:{Status}.", dr.Key, dr.Value.Status);
			}
		}

		private TestObject GetTestObject()
		{
			Console.ForegroundColor = ConsoleColor.Red;
			Console.WriteLine("Enter Message Key: ");
			var key = Console.ReadLine();
			Console.WriteLine("Enter New Status: ");
			var status = Console.ReadLine();
			Console.ForegroundColor = ConsoleColor.White;

			return new TestObject
			{
				IdNumber = key,
				Status = status
			};
		}
	}
}
