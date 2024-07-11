using Confluent.SchemaRegistry.Serdes;
using KafkaFlow;
using KafkaFlow.Configuration;
using KafkaFlow.Middlewares.Serializer.Resolvers;
using KafkaFlow.Serializer.SchemaRegistry;
using NJsonSchema.Generation;

internal static class JsonSerialization
{
    /// <summary>
    /// Registers a middleware to serialize json messages using schema registry
    /// </summary>
    /// <param name="middlewares">The middleware configuration builder</param>
    /// <param name="config">The protobuf serializer configuration</param>
    /// <typeparam name="TMessage">The message type</typeparam>
    /// <returns></returns>
    public static IProducerMiddlewareConfigurationBuilder AddClusterSpecificSchemaRegistryJsonSerializer<TMessage>(
        this IProducerMiddlewareConfigurationBuilder middlewares,
        JsonSerializerConfig config = null)
    {
        return middlewares.AddSerializer(
            resolver =>
            {
                var schemaRegistryClientCache = new SchemaRegistryClientCache(resolver);
                return new ConfluentClusterSpecificJsonSerializer(schemaRegistryClientCache, config);
            },
            _ => new SingleMessageTypeResolver(typeof(TMessage)));
    }


    /// <summary>
    /// A json message serializer integrated with the confluent schema registry
    /// This is a copy of https://github.com/Farfetch/kafkaflow/blob/master/src/KafkaFlow.Serializer.SchemaRegistry.ConfluentJson/ConfluentJsonSerializer.cs
    /// except it resolves the SchemaRegistryClient correctly
    /// </summary>
    internal class ConfluentClusterSpecificJsonSerializer : ISerializer
    {
        private readonly ISchemaRegistryClientCache _clientCache;
        private readonly JsonSerializerConfig _serializerConfig;
        private readonly JsonSchemaGeneratorSettings _schemaGeneratorSettings;

        public ConfluentClusterSpecificJsonSerializer(ISchemaRegistryClientCache clientCache,
            JsonSerializerConfig serializerConfig = null)
            : this(
                clientCache,
                serializerConfig,
                null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ConfluentJsonSerializer"/> class.
        /// </summary>
        /// <param name="clientCache">Cache of SchemaRegistryClients</param>
        /// <param name="serializerConfig">An instance of <see cref="JsonSerializerConfig"/></param>
        /// <param name="schemaGeneratorSettings">An instance of <see cref="JsonSchemaGeneratorSettings"/></param>
        public ConfluentClusterSpecificJsonSerializer(
            ISchemaRegistryClientCache clientCache,
            JsonSerializerConfig serializerConfig,
            JsonSchemaGeneratorSettings schemaGeneratorSettings = null)
        {
            _clientCache = clientCache;
            _serializerConfig = serializerConfig;
            _schemaGeneratorSettings = schemaGeneratorSettings;
        }

        /// <inheritdoc/>
        public Task SerializeAsync(object message, Stream output, ISerializerContext context)
        {
            return ConfluentSerializerWrapper
                .GetOrCreateSerializer(
                    message.GetType(),
                    () => Activator.CreateInstance(
                        typeof(JsonSerializer<>).MakeGenericType(message.GetType()),
                        _clientCache.GetSchemaRegistryClientByMessageFullName(message.GetType().FullName).Client,
                        _serializerConfig,
                        _schemaGeneratorSettings))
                .SerializeAsync(message, output, context);
        }
    }
}