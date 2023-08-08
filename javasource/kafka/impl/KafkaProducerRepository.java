package kafka.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaProducerRepository {
	private static Map<String, KafkaProducer<String, String>> producers = new HashMap<>(); 	
	
	public static void put(String name, KafkaProducer<String, String> producer) {
		producers.put(name, producer);
	}
	
	public static KafkaProducer<String, String> get(String name)
	{
		return producers.get(name);
	}
	
	public static void close(String name) {
		KafkaProducer<String, String> producer = producers.get(name);
		if (producer != null) {
			producer.close();
		}	
	}
	
	public static void closeAll()
	{
		for (KafkaProducer<String, String> producer : producers.values()) {
			producer.close();
		}
		producers.clear();
	}
}
