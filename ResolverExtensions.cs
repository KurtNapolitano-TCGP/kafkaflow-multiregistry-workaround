using Confluent.SchemaRegistry;
using KafkaFlow;

/// <summary>
/// Extension methods to ensure we can resolve the correct instance of ISchemaRegistryClient based on cluster name
/// </summary>
public static class ResolverExtensions
{
    /// <summary>
    /// Get an instance of ISchemaRegistryClient associated with the provided cluster name.
    /// </summary>
    public static ISchemaRegistryClient ResolveClusterSpecificSchemaRegistryClient(this IDependencyResolver resolver,
        string clusterName)
    {
        // Get all the providers we've registered
        var providers = resolver.ResolveAll(typeof(SchemaRegistryClientProvider));

        // Loop through them to find the one that matches our cluster name
        var provider = providers.FirstOrDefault(p =>
        {
            if (p is not SchemaRegistryClientProvider provider)
                return false;

            return provider.ClusterName == clusterName;
        });

        if (provider is not SchemaRegistryClientProvider clientProvider)
        {
            throw new UnknownClusterException(clusterName);
        }

        return clientProvider.ResolveClient(resolver);
    }

    private class UnknownClusterException : Exception
    {
        public UnknownClusterException(string clusterName) : base(
            $"No cluster specific schema registry was found for a cluster named {clusterName}.")
        { }
    }
}