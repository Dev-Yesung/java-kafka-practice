package com.udemy.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallBack {

	private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallBack.class.getSimpleName());

	public static void main(String[] args) {
		log.info("I am a Kafka Producer");
		// create Producer Properties
		// 자바에서 제공해주는 Properties 클래스로 설정 가능 -> 내부에 HashTable로 구현되어 있음
		var properties = new Properties();
		// connect to localhost
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		// set key/value serializer
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		// batch size -> 상용 환경에서는 사용 X -> 카프카의 기본 배치사이즈는 16KB 이다.
		// properties.setProperty("batch.size", "400");

		// partitioner.class 를 설정할 수 있다. 하지만 상용 환경에서는 사용 X
		// properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

		// create the Producer
		try (
			var producer = new KafkaProducer<String, String>(properties)
		) {
			// 하나의 배치 메시지를 보내면 Sticky Partitioner 방식에 의해 파티션으로 들어간다.
			// 이 방식으로 메시지를 생성할 경우 하나의 Partition 에만 메시지를 보낸다.
			// Sticky Partitioner 방식은 별 다른 세팅을 하지 않았을 때 동작하는 방식이다.
			// 기존의 Round Robin 방식에 비해 Sticky Partitioner 방식의 성능이 더 우수하다.
			for (int j = 0; j < 10; j++) {
				// 만일 하나가 아니라 여러 개의 배치 메시지를 보낼 경우 어떻게 동작할까?
				// 여러 파티션에 배치 메시지들이 돌아가면서 들어가게 된다.
				for (int i = 0; i < 30; i++) {
					var producerRecord = new ProducerRecord<String, String>("demo_java", "hello world " + i);
					// send 메서드의 두 번쨰 인자로 Callback 클래스를 넘겨줄 수 있다.
					producer.send(producerRecord, (metadata, e) -> {
						// execute every time a record successfully sent or an exception is thrown
						if (e != null) {
							log.error("Error while producing", e);
						}
						// the record was successfully sent
						log.info("Received new metadata\n" +
								 "Topic: " + metadata.topic() + "\n" +
								 "Partition: " + metadata.partition() + "\n" +
								 "Offset: " + metadata.offset() + "\n" +
								 "Timestamp: " + metadata.timestamp() + "\n"
						);
					});
				}

				Thread.sleep(500);
			}

			producer.flush();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
