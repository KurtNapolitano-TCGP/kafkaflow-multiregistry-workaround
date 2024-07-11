using Confluent.SchemaRegistry;
using KafkaFlow.Configuration;
using KafkaFlow;

internal static class WithSchemaRegistry
{
    /// <summary>
    /// This method exists because there's a bug in KafkaFlow. https://github.com/Farfetch/kafkaflow/issues/558
    /// To work around that, we've added this method which registers a specific schema registry client tied to a cluster.
    /// </summary>
    public static IClusterConfigurationBuilder WithClusterSpecificSchemaRegistry(
        this IClusterConfigurationBuilder cluster,
        string clusterName,
        Action<SchemaRegistryConfig> handler)
    {
        var config = new SchemaRegistryConfig();
        handler(config);

        Func<IDependencyResolver, ISchemaRegistryClient> factory = _ => new CachedSchemaRegistryClient(config);

        // Continue to register the singleton like we always did so that anyone not using multiple clusters can still use the built-in serializers.
        cluster.DependencyConfigurator.AddSingleton(factory);

        // Also register a provider with the same factory but including the clusterName
        cluster.DependencyConfigurator.AddSingleton(new SchemaRegistryClientProvider(clusterName, factory));

        return cluster;
    }
}