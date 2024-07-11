using System.Buffers.Binary;
using System.Collections.Concurrent;
using KafkaFlow.Middlewares.Serializer.Resolvers;
using KafkaFlow;
using Confluent.SchemaRegistry;
using Newtonsoft.Json;

/// <summary>
///  The message type resolver to be used with schema registry serializers
/// This is a reimplementation of SchemaRegistryTypeResolver which keeps a different cache of schema ids PER CLUSTER
/// </summary>
internal class ClusterSpecificSchemaRegistryTypeResolver : IMessageTypeResolver
{
    private static readonly ConcurrentDictionary<string, ConcurrentDictionary<int, Type>> _schemaTypesByClusterAndId = new();
    private static readonly SemaphoreSlim s_semaphore = new(1, 1);

    private readonly ISchemaRegistryClientCache _clientCache;

    public ClusterSpecificSchemaRegistryTypeResolver(ISchemaRegistryClientCache clientCache)
    {
        _clientCache = clientCache;
    }

    /// <inheritdoc />
    public async ValueTask<Type> OnConsumeAsync(IMessageContext context)
    {
        var schemaId = BinaryPrimitives.ReadInt32BigEndian(
            ((byte[])context.Message.Value).AsSpan().Slice(1, 4));


        var (clusterName, client) = _clientCache.GetSchemaRegistryClientByConsumerName(context.ConsumerContext.ConsumerName);

        if (_schemaTypesByClusterAndId.TryGetValue(clusterName, out var mapBySchemaId) && mapBySchemaId.TryGetValue(schemaId, out var type))
        {
            return type;
        }

        await s_semaphore.WaitAsync();

        try
        {
            if (!_schemaTypesByClusterAndId.TryGetValue(clusterName, out mapBySchemaId))
            {
                mapBySchemaId = new();
                _schemaTypesByClusterAndId[clusterName] = mapBySchemaId;
            }

            if (mapBySchemaId.TryGetValue(schemaId, out type))
            {
                return type;
            }

            var resolver = new ConfluentAvroTypeNameResolver(client);
            var typeName = await resolver.ResolveAsync(schemaId);

            return mapBySchemaId[schemaId] = AppDomain.CurrentDomain
                .GetAssemblies()
                .Select(a => a.GetType(typeName))
                .FirstOrDefault(x => x != null);
        }
        finally
        {
            s_semaphore.Release();
        }
    }

    /// <inheritdoc />
    public ValueTask OnProduceAsync(IMessageContext context) => default(ValueTask);

    /// <summary>
    /// This is a carbon copy of the internal-only class of the exact same name in KafkaFlow:
    /// https://github.com/Farfetch/kafkaflow/blob/master/src/KafkaFlow.Serializer.SchemaRegistry.ConfluentAvro/ConfluentAvroTypeNameResolver.cs
    /// We need to instantiate it above, so we need an instance of it that is accessible
    /// </summary>
    private class ConfluentAvroTypeNameResolver : ISchemaRegistryTypeNameResolver
    {
        private readonly ISchemaRegistryClient _client;

        public ConfluentAvroTypeNameResolver(ISchemaRegistryClient client)
        {
            _client = client;
        }

        public async Task<string> ResolveAsync(int id)
        {
            var schema = await _client.GetSchemaAsync(id);

            var avroFields = JsonConvert.DeserializeObject<AvroSchemaFields>(schema.SchemaString);
            return $"{avroFields.Namespace}.{avroFields.Name}";
        }

        private class AvroSchemaFields
        {
            public string Name { get; set; }

            public string Namespace { get; set; }
        }
    }
}