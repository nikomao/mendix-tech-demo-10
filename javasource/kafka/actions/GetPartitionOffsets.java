// This file was generated by Mendix Studio Pro.
//
// WARNING: Only the following code will be retained when actions are regenerated:
// - the import list
// - the code between BEGIN USER CODE and END USER CODE
// - the code between BEGIN EXTRA CODE and END EXTRA CODE
// Other code you write will be lost the next time you deploy the project.
// Special characters, e.g., é, ö, à, etc. are supported in comments.

package kafka.actions;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.webui.CustomJavaAction;
import kafka.impl.KafkaModule;
import kafka.impl.KafkaPropertiesFactory;
import kafka.proxies.Partition;
import com.mendix.systemwideinterfaces.core.IMendixObject;

public class GetPartitionOffsets extends CustomJavaAction<java.lang.Boolean>
{
	/** @deprecated use com.mendix.utils.ListUtils.map(partitions, com.mendix.systemwideinterfaces.core.IEntityProxy::getMendixObject) instead. */
	@java.lang.Deprecated(forRemoval = true)
	private final java.util.List<IMendixObject> __partitions;
	private final java.util.List<kafka.proxies.Partition> partitions;
	/** @deprecated use consumer.getMendixObject() instead. */
	@java.lang.Deprecated(forRemoval = true)
	private final IMendixObject __consumer;
	private final kafka.proxies.Consumer consumer;

	public GetPartitionOffsets(
		IContext context,
		java.util.List<IMendixObject> _partitions,
		IMendixObject _consumer
	)
	{
		super(context);
		this.__partitions = _partitions;
		this.partitions = java.util.Optional.ofNullable(_partitions)
			.orElse(java.util.Collections.emptyList())
			.stream()
			.map(partitionsElement -> kafka.proxies.Partition.initialize(getContext(), partitionsElement))
			.collect(java.util.stream.Collectors.toList());
		this.__consumer = _consumer;
		this.consumer = _consumer == null ? null : kafka.proxies.Consumer.initialize(getContext(), _consumer);
	}

	@java.lang.Override
	public java.lang.Boolean executeAction() throws Exception
	{
		// BEGIN USER CODE
		Properties kafkaProps = KafkaPropertiesFactory.getKafkaProperties(getContext(), consumer);
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(kafkaProps);
		try {
			List<TopicPartition> topicPartitions = new LinkedList<>();
			for (Partition partition : partitions) {
				TopicPartition topicPartition = new TopicPartition(partition.getPartition_Topic().getName(), partition.getIdentifier());
				topicPartitions.add(topicPartition);
			}
			Map<TopicPartition, Long> beginnings = kafkaConsumer.beginningOffsets(topicPartitions, Duration.ofMillis(10000));
			Map<TopicPartition, Long> endings = kafkaConsumer.endOffsets(topicPartitions, Duration.ofMillis(10000));
			for (Entry<TopicPartition, Long> beginning : beginnings.entrySet()) {
				for (Partition partition : partitions) {
					if (partition.getPartition_Topic().getName().equals(beginning.getKey().topic()) &&
						partition.getIdentifier() == beginning.getKey().partition()) {
						partition.setBeginOffset(beginning.getValue());
					}
				}
			}
			
			for (Entry<TopicPartition, Long> ending : endings.entrySet()) {
				for (Partition partition : partitions) {
					if (partition.getPartition_Topic().getName().equals(ending.getKey().topic()) &&
						partition.getIdentifier() == ending.getKey().partition()) {
						partition.setEndOffset(ending.getValue());
					}
				}
			}
			
		} catch (Exception e) {
			KafkaModule.LOGGER.debug("Error while obtaining list of partition offsets from server " + consumer.getName() + ": " + e.getMessage(), e);
			throw e;
		} finally {
			kafkaConsumer.close();
		}
		return true;
		// END USER CODE
	}

	/**
	 * Returns a string representation of this action
	 * @return a string representation of this action
	 */
	@java.lang.Override
	public java.lang.String toString()
	{
		return "GetPartitionOffsets";
	}

	// BEGIN EXTRA CODE
	// END EXTRA CODE
}
