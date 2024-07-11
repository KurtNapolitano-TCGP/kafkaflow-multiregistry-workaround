using Confluent.SchemaRegistry;
using KafkaFlow;

internal class SchemaRegistryClientCache : ISchemaRegistryClientCache
{
    private readonly Dictionary<string, ISchemaRegistryClient> _schemaRegistryClients = new();
    private readonly Dictionary<string, string> _clusterNameByMessageFullName = new();
    private readonly Dictionary<string, string> _clusterNameByConsumerName = new();
    public SchemaRegistryClientCache(IDependencyResolver resolver)
    {
        var providers = resolver.ResolveAll(typeof(SchemaRegistryClientProvider));
        foreach (var p in providers)
        {
            if (p is not SchemaRegistryClientProvider provider)
                continue;

            _schemaRegistryClients.Add(provider.ClusterName, provider.ResolveClient(resolver));
        }

        var messageDefinitions = resolver.ResolveAll(typeof(MessageDefinition));
        foreach (var m in messageDefinitions)
        {
            if (m is not MessageDefinition definition)
                continue;

            _clusterNameByMessageFullName.Add(definition.MessageFullName, definition.ClusterName);
        }

        var consumerDefinitions = resolver.ResolveAll(typeof(ConsumerDefinition));
        foreach (var c in consumerDefinitions)
        {
            if (c is not ConsumerDefinition definition)
                continue;

            _clusterNameByConsumerName.Add(definition.ConsumerName, definition.ClusterName);
        }
    }

    public (string ClusterName, ISchemaRegistryClient Client) GetSchemaRegistryClientByConsumerName(string consumerName)
    {

        if (!_clusterNameByConsumerName.TryGetValue(consumerName, out var clusterName))
        {
            throw new InvalidOperationException(
                $"No consumer definition was found for {consumerName}.");
        }

        if (!_schemaRegistryClients.TryGetValue(clusterName, out var schemaRegistryClient))
        {
            throw new InvalidOperationException(
                $"No schema registry configuration was found for cluster {clusterName}. Set it using {nameof(ClusterConfigurationBuilderExtensions.WithSchemaRegistry)} on cluster configuration");
        }

        return (clusterName, schemaRegistryClient);
    }

    public (string ClusterName, ISchemaRegistryClient Client) GetSchemaRegistryClientByMessageFullName(string messageTypeFullName)
    {
        if (!_clusterNameByMessageFullName.TryGetValue(messageTypeFullName, out var clusterName))
        {
            throw new InvalidOperationException(
                $"No message definition was found for {messageTypeFullName}.");
        }

        if (!_schemaRegistryClients.TryGetValue(clusterName, out var schemaRegistryClient))
        {
            throw new InvalidOperationException(
                $"No schema registry configuration was found for cluster {clusterName}. Set it using {nameof(ClusterConfigurationBuilderExtensions.WithSchemaRegistry)} on cluster configuration");
        }

        return (clusterName, schemaRegistryClient);
    }
}