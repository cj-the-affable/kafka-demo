using System;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafka.Demo.Consumer
{
	class Program
	{
		static void Main(string[] args)
		{
			Console.Title = "Consumer";
			var hostbuilder = Host.CreateDefaultBuilder(args)
			.ConfigureServices((ctx, services) => {
				services.Configure<ConsumerConfig>(ctx.Configuration.GetSection("ConsumerConfig"));
				services.AddHostedService<MagicConsumer>();

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
			var host = hostbuilder.Build();
			host.Run();
		}
	}
}
