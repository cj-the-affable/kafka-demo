using System;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafka.Demo.Producer
{
	class Program
	{
		static void Main(string[] args)
		{
			Console.Title = "Producer";
			var hostbuilder = Host.CreateDefaultBuilder(args)
			.ConfigureServices((ctx, services) => {
				services.Configure<ProducerConfig>(ctx.Configuration.GetSection("ProducerConfig"));
				services.AddHostedService<MagicProducer>();

			})
			.ConfigureLogging((ctx, builder) => {
				builder.AddJsonConsole(options => {
					options.IncludeScopes = false;
					options.TimestampFormat = "hh:mm";
					options.JsonWriterOptions = new JsonWriterOptions
					{
						Indented = true
					};					
				});
			});
			hostbuilder.Build().Run();
		}
	}
}
