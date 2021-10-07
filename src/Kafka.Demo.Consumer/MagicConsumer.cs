using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.Demo.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Serializers = Kafka.Demo.Models.Serializers;

namespace Kafka.Demo.Consumer
{
	public class MagicConsumer : BackgroundService
	{
		private readonly ConsumerConfig ProducerConfig;
		private readonly ILogger Logger;

		public MagicConsumer(IOptionsSnapshot<ConsumerConfig> producerConfig, ILogger<MagicConsumer> logger)
		{
			ProducerConfig = producerConfig.Value;
			Logger = logger;
		}

		protected override Task ExecuteAsync(CancellationToken stoppingToken)
		{
			StartConsumingMessages(stoppingToken);
			return Task.CompletedTask;
		}

		private async Task StartConsumingMessages(CancellationToken stoppingToken)
		{
			try
			{
				Logger.LogInformation("Starting to consume messages.");
				await ConsumeMessages(stoppingToken);
				Logger.LogInformation("Stopped consuming messages.");
			}
			catch (Exception e)
			{
				Logger.LogError($"Uncaught exception.", e.Message);
			}
		}

		private async Task ConsumeMessages(CancellationToken stoppingToken)
		{
			var consumerBuilder = new ConsumerBuilder<string, TestObject>(ProducerConfig);
			consumerBuilder.SetKeyDeserializer(new Serializers.JsonSerializer());
			consumerBuilder.SetValueDeserializer(new Serializers.JsonSerializer());

			var maxTimeToWaitForMessage = TimeSpan.FromSeconds(30);
			using (var consumer = consumerBuilder.Build())
			{
				consumer.Subscribe("test_topic"); //Don't forget this

				while (!stoppingToken.IsCancellationRequested)
				{
					var msgContext = consumer.Consume(maxTimeToWaitForMessage); //Returns null when max time elapses without new msg

					if (msgContext == null)
					{
						Logger.LogInformation("No messages to process.", maxTimeToWaitForMessage);
						continue;
					}

					Logger.LogInformation("Received message. {Key}:{Status}.", msgContext.Message.Key, msgContext.Message.Value.Status);
				}
			}
		}
	}
}
