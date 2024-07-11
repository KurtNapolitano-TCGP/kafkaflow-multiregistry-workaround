using Confluent.SchemaRegistry;

internal interface ISchemaRegistryClientCache
{
    (string ClusterName, ISchemaRegistryClient Client) GetSchemaRegistryClientByConsumerName(string consumerName);

    (string ClusterName, ISchemaRegistryClient Client) GetSchemaRegistryClientByMessageFullName(
        string messageTypeFullName);
}