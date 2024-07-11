using Confluent.SchemaRegistry;
using KafkaFlow;

internal class SchemaRegistryClientProvider
{
    public string ClusterName { get; }
    private readonly Func<IDependencyResolver, ISchemaRegistryClient> _factory;

    public SchemaRegistryClientProvider(string clusterName, Func<IDependencyResolver, ISchemaRegistryClient> factory)
    {
        ClusterName = clusterName;
        _factory = factory;
    }

    public ISchemaRegistryClient ResolveClient(IDependencyResolver resolver) => _factory(resolver);

}