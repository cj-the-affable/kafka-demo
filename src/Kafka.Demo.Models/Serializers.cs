using System;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;

namespace Kafka.Demo.Models
{
	public static class Serializers
	{
		public class JsonSerializer 
			: ISerializer<TestObject>, IDeserializer<TestObject>
			, ISerializer<string>, IDeserializer<string>
		{
			private static readonly JsonSerializerOptions SerializerOptions = new JsonSerializerOptions
			{
				PropertyNameCaseInsensitive = true
			};

			public byte[] Serialize(TestObject data, SerializationContext context)
			{
				return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(data);
			}

			public TestObject Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
			{
				var json = Encoding.UTF8.GetString(data);
				var deserialized = System.Text.Json.JsonSerializer.Deserialize<TestObject>(json, SerializerOptions);
				return deserialized;
			}

			public byte[] Serialize(string data, SerializationContext context)
			{
				return System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(data);
			}

			string IDeserializer<string>.Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
			{
				if (data.Length > 0)
				{
					return System.Text.Json.JsonSerializer.Deserialize<string>(data, SerializerOptions);
				}
				else
				{
					return null;
				}
			}
		}
	}
}
