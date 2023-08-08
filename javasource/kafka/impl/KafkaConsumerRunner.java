package kafka.impl;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IDataType;

import kafka.proxies.CommitControl;
import kafka.proxies.Consumer;
import kafka.proxies.constants.Constants;

public class KafkaConsumerRunner extends KafkaConfigurable implements Runnable {
	private final AtomicBoolean stopped = new AtomicBoolean(false);
	private final KafkaConsumer<String, String> consumer;
	private final String onReceiveMicroflow;
	private final Map<String, IDataType> onReceiveInputParameters;
	private final CommitControl commitControl;

	// Domain model objects
	private final Consumer consumerDom;

	private final String name;

	public KafkaConsumerRunner(Consumer consumer, IContext context) throws CoreException {
		super(context);
		this.consumerDom = consumer;
		this.name = consumer.getName();
		this.props = KafkaPropertiesFactory.getKafkaProperties(context, consumer);
		this.commitControl = consumerDom.getCommitControl();
		switch (commitControl) {
		case CONSUMER:
			props.put("enable.auto.commit", "false");
			break;
		case SERVER:
			props.put("enable.auto.commit", "true");
			break;
		default:
			LOGGER.critical("Consumer " + name + " contains an invalid commit control.");
		}
		
		this.consumer = new KafkaConsumer<>(props);
		this.context = context;
		this.onReceiveMicroflow = consumer.getOnReceiveMicroflow();
		this.onReceiveInputParameters = Core.getInputParameters(this.onReceiveMicroflow);
	}

	public void run() {
		consumer.subscribe(Arrays.asList(this.consumerDom.getTopics().split(";")));
		while (!stopped.get()) {
			try {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					Map<String, Object> microflowParams = new HashMap<String, Object>();
					if (this.onReceiveInputParameters.containsKey("Offset")) {
						microflowParams.put("Offset", record.offset());
					}
					if (this.onReceiveInputParameters.containsKey("Key")) {
						microflowParams.put("Key", record.key());
					}
					if (this.onReceiveInputParameters.containsKey("Value")) {
						microflowParams.put("Value", record.value());
					}
					if (this.onReceiveInputParameters.containsKey("Partition")) {
						microflowParams.put("Partition", record.partition());
					}

					for (Header header : record.headers()) {
						try {
							if (this.onReceiveInputParameters.containsKey(header.key())) {
								microflowParams.put(header.key(), new String(header.value()));
							}
						} catch (Exception e) {
							LOGGER.warn("Ignoring header " + header.key() + " for offset " + record.offset()
									+ " because it has an invalid value.");
						}
					}
					
					IContext context = Core.createSystemContext();
					try {
						Core.microflowCall(onReceiveMicroflow).withParams(microflowParams).execute(context);	// throws CoreException
						while (context.isInTransaction())
							context.endTransaction();
					} catch (Throwable e) {
						LOGGER.error("An error occurred while executing the microflow for consumer " + name);
						try {
							while (context.isInTransaction())
								context.endTransaction();
						} catch (Exception ex) {};
					} 
				}
				if (commitControl == CommitControl.CONSUMER) {
					consumer.commitSync();
				}
			} catch (WakeupException e) {
				// Ignore exception if closing
				if (!stopped.get()) throw e;
			} catch (Exception e) {
				String msg = "An uncatched exception occurred on Kafka consumer " + name + " expect to (temporary) have a consumer less.";
				if(Constants.getLogConsumerLostAsCritical()) {
					LOGGER.critical(msg, e);
				} else {
					LOGGER.error(msg, e);
				}
				
				try { Thread.sleep(30000); } catch (Exception ex) {}
			}
		}

	}

	// Shutdown hook which can be called from a separate thread
	public void stop() {
		stopped.set(true);
		consumer.wakeup();
	}
}
