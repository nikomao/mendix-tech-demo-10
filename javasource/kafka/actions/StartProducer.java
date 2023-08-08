// This file was generated by Mendix Studio Pro.
//
// WARNING: Only the following code will be retained when actions are regenerated:
// - the import list
// - the code between BEGIN USER CODE and END USER CODE
// - the code between BEGIN EXTRA CODE and END EXTRA CODE
// Other code you write will be lost the next time you deploy the project.
// Special characters, e.g., é, ö, à, etc. are supported in comments.

package kafka.actions;

import org.apache.kafka.clients.producer.KafkaProducer;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.webui.CustomJavaAction;
import kafka.impl.KafkaProducerRepository;
import kafka.impl.KafkaPropertiesFactory;
import com.mendix.systemwideinterfaces.core.IMendixObject;

/**
 * Starts a Kafka producer.
 * 
 * After the producer has started, you can send ('produce') messages with the Send action.
 * 
 * This action will always return true.
 */
public class StartProducer extends CustomJavaAction<java.lang.Boolean>
{
	/** @deprecated use producer.getMendixObject() instead. */
	@java.lang.Deprecated(forRemoval = true)
	private final IMendixObject __producer;
	private final kafka.proxies.Producer producer;

	public StartProducer(
		IContext context,
		IMendixObject _producer
	)
	{
		super(context);
		this.__producer = _producer;
		this.producer = _producer == null ? null : kafka.proxies.Producer.initialize(getContext(), _producer);
	}

	@java.lang.Override
	public java.lang.Boolean executeAction() throws Exception
	{
		// BEGIN USER CODE
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(
					KafkaPropertiesFactory.getKafkaProperties(getContext(), producer));
		KafkaProducerRepository.put(producer.getName(), kafkaProducer);
		
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
		return "StartProducer";
	}

	// BEGIN EXTRA CODE
	// END EXTRA CODE
}
