package com.udemy.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithKeys {

	private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

	/*
		Key 설정을 하게 되면 특정 Key 는 특정 파티션으로 들어가게 된다.
	 */
	public static void main(String[] args) {
		var properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());

		log.info("I am a Kafka Producer");
		try (
			var producer = new KafkaProducer<String, String>(properties)
		) {
			// 같은 key 값을 가진 메시지들이 같은 파티션으로 가는지 확인
			// 이때 파티션에 들어가는 메시지는 순서가 다르더라도 배치처리 된다.
			for (int j = 0; j < 2; j++) {
				for (int i = 0; i < 10; i++) {
					String topic = "demo_java";
					String key = "id_" + i;
					String value = "hello world" + i;

					var producerRecord =
						new ProducerRecord<>(topic, key, value);

					producer.send(producerRecord, (metadata, e) -> {
						if (e != null) {
							log.error("Error while producing", e);
						}
						// the record was successfully sent
						log.info("Key: " + key + " | " + "Partition: " + metadata.partition());
					});
				}

				Thread.sleep(500);
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
