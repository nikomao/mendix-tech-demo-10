package kafka.impl;

import java.util.HashMap;
import java.util.Map;

public class KafkaConsumerRepository {
	private static Map<String, KafkaConsumerRunner> consumers = new HashMap<String, KafkaConsumerRunner>(); 	

	public static void put(String name, KafkaConsumerRunner consumer) {
		consumers.put(name, consumer);
	}
	
	public static void stop(String name)
	{
		consumers.get(name).stop();
	}

	public static void stopAll()
	{
		for (KafkaConsumerRunner consumer : consumers.values()) {
			consumer.stop();
		}
		consumers.clear();
	}
}
