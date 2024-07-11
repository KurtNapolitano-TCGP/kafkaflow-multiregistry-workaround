# Multi-SchemaRegistry Workaround

This entire folder/namespace exists because https://github.com/Farfetch/kafkaflow/issues/558 has not been resolved.  When it is resolved.  This namespace can (and should) be deleted.

The majority of classes you'll find here are copies of KafkaFlow classes with re-implementations that allow us to take advantage of Cluster-specific schema registries.

## Why?

In order for an application to communicate with more than one kafka cluster, it must associate different schema registries for each of them.  KafkaFlow (incorrectly) adds the `ISchemaRegistryClient` as a singleton which results in always using whichever the last one registered was.

## Okay, so how does this all work?

This whole thing is accomplished by using KafkaFlow's embedded DI container and registering instances of some newly created classes:

- SchemaRegistryClientProvider
- ConsumerDefinition
- MessageDefinition

The differentiating factor between each of the clusters we've called "clusterName".

### SchemaRegistryClientProvider

When we call `.WithClusterSpecificSchemaRegistry` instead of `.WithSchemaRegistry`, a `SchemaRegistryClientProvider` is created and registered.  This is simply a class which is aware of a ClusterName and has an `ISchemaRegistryClient` to provide.


### ConsumerDefinition

A consumer definition is a pairing of the ClusterName with the ConsumerName.

When a consumer is created, we give it a name (if we didn't it would get a random GUID, so we just give it a random GUID that we _know_).  A consumer definition is then created and registered.

When consuming a message, our custom re-implemented deserializers will use the consumer name to look up the cluster name by resolving the entire list of consumer definitions and finding the one that matches the consumer name of the incoming message.

Using the cluster name, we can resolve all registered instances of `SchemaRegistryClientProvider` and select the appropriate one for this cluster.

### MessageDefinition

A message definition is a pairing of the ClusterName with the Message class full name.

When a producer or consumer of a message is created, a message definition is created and registered for that message.

When producing a message, our custom re-implemented serializers will use the message type to look up the cluster name by resolving the entire list of message definitions and finding the one that matches the message type being produced.

Using the cluster name we can resolve all registered instances of `SchemaRegistryClientProvider` and select the appropriate one for this cluster.

_Note: This introduces the limitation that a single message class **cannot** be used to produce a message for more than one cluster.  We've all agreed this is a reasonable limitation and you probably shouldn't do this anyway._

### Caching

The `ClusterSpecificSchemaRegistryTypeResolver` is a reimplementation of the `SchemaRegistryTypeResolver`, it caches types to schema ids PER cluster instead of just a flat map of type to schema id.

The `SchemaRegistryClientCache` exists because while resolving _every_ instance of `ConsumerDefinition`, `MessageDefinition`, or `SchemaRegistryClientProvider` isn't extremely expensive, it's a lot cheaper (and simplifies the code) if we do it only once instead of on every publish/consume, and keep our own dictionary to look up the correct `ISchemaRegistryClient`.


