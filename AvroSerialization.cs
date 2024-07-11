using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using KafkaFlow;
using KafkaFlow.Configuration;
using KafkaFlow.Middlewares.Serializer;
using KafkaFlow.Serializer.SchemaRegistry;
using Newtonsoft.Json;

internal static class AvroSerialization
{
    /// <summary>
    /// Registers a middleware to serialize avro messages using schema registry
    /// This is the exact same logic as https://github.com/Farfetch/kafkaflow/blob/master/src/KafkaFlow.Serializer.SchemaRegistry.ConfluentAvro/ProducerConfigurationBuilderExtensions.cs
    /// except for the schema registry client resolution. 
    /// </summary>
    /// <param name="middlewares">The middleware configuration builder</param>
    /// <param name="clusterName">The cluster name</param>
    /// <param name="config">The avro serializer configuration</param>
    /// <returns></returns>
    public static IProducerMiddlewareConfigurationBuilder AddClusterSpecificSchemaRegistryAvroSerializer(
        this IProducerMiddlewareConfigurationBuilder middlewares,
        AvroSerializerConfig config = null)
    {
        return middlewares.Add(
            resolver =>
            {
                var schemaRegistryClientCache = new SchemaRegistryClientCache(resolver);
                return 
                    new SerializerProducerMiddleware(
                    new ConfluentClusterSpecificAvroSerializer(schemaRegistryClientCache, config),
                    new ClusterSpecificSchemaRegistryTypeResolver(schemaRegistryClientCache));
            });
    }

    /// <summary>
    /// Same logic as AddSchemaRegistryAvroDeserializer except it correctly resolves the cluster specific ISchemaRegistryClient
    /// </summary>
    public static IConsumerMiddlewareConfigurationBuilder AddClusterSpecificSchemaRegistryAvroDeserializer(
        this IConsumerMiddlewareConfigurationBuilder middlewares)
    {
        return middlewares.Add(
            resolver =>
            {
                var schemaRegistryClientCache = new SchemaRegistryClientCache(resolver);
                return 
                    new DeserializerConsumerMiddleware(
                    new ConfluentClusterSpecificAvroDeserializer(schemaRegistryClientCache),
                    new ClusterSpecificSchemaRegistryTypeResolver(schemaRegistryClientCache));
            });
    }



    /// <summary>
    /// A message serializer using Apache.Avro library
    /// This is a copy of <see cref="ConfluentAvroSerializer"/> with the addition of resolving the schemaRegistryClient correctly
    /// </summary>
    private class ConfluentClusterSpecificAvroSerializer : ISerializer
    {
        private readonly AvroSerializerConfig _serializerConfig;
        private readonly ISchemaRegistryClientCache _schemaRegistryClientCache;

        public ConfluentClusterSpecificAvroSerializer(
            ISchemaRegistryClientCache clientCache,
            AvroSerializerConfig serializerConfig = null)
        {
            _schemaRegistryClientCache = clientCache;
            _serializerConfig = serializerConfig;
        }

        /// <inheritdoc/>
        public Task SerializeAsync(object message, Stream output, ISerializerContext context)
        {
            
            return ConfluentSerializerWrapper
                .GetOrCreateSerializer(
                    message.GetType(),
                    () => Activator.CreateInstance(
                        typeof(AvroSerializer<>).MakeGenericType(message.GetType()),
                        _schemaRegistryClientCache.GetSchemaRegistryClientByMessageFullName(message.GetType().FullName).Client,
                        _serializerConfig))
                .SerializeAsync(message, output, context);
        }
    }

    /// <summary>
    /// A message serializer using Apache.Avro library
    /// This is a copy of <see cref="ConfluentAvroSerializer"/> with the addition of resolving the schemaRegistryClient correctly
    /// </summary>
    private class ConfluentClusterSpecificAvroDeserializer : IDeserializer
    {
        private readonly ISchemaRegistryClientCache _schemaRegistryClientCache;

        public ConfluentClusterSpecificAvroDeserializer(ISchemaRegistryClientCache clientCache)
        {
            _schemaRegistryClientCache = clientCache;
        }

        /// <inheritdoc/>
        public Task<object> DeserializeAsync(Stream input, Type type, ISerializerContext context)
        {
            return ConfluentDeserializerWrapper
                .GetOrCreateDeserializer(
                    type,
                    () => Activator
                        .CreateInstance(
                            typeof(AvroDeserializer<>).MakeGenericType(type),
                            _schemaRegistryClientCache.GetSchemaRegistryClientByMessageFullName(type.FullName).Client,
                            null))
                .DeserializeAsync(input, context);
        }
    }




}