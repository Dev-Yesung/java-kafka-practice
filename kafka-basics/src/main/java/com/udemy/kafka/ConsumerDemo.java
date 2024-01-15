package com.udemy.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
	private static final String GROUP_ID = "my-java-application";
	private static final String TOPIC = "demo_java";

	/*
		Kafka 는 poll 이란 메서드를 통해 데이터를 받을 수 있다.
		메시지를 받고나서 바로 응답 데이터를 반환할 수도 있지만,
		일정 시간 이후(timeout) 빈 값으로 반환할 수도 있다.
	 */
	public static void main(String[] args) {
		var properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

		// Consumer 의 경우, 값을 사용하기 때문에 deserialize 해야 한다. 따라서 아래와 같이 설정한다.
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());

		// Consumer Group 을 설정해서 받기 위해 아래와 같이 Group id를 세팅한다.
		properties.setProperty("group.id", GROUP_ID);

		// Consumer 가 프로퍼티의 offset 을 리셋하는 방법 : none/earliest/latest 가 있다.
		// none 속성은 Consumer Group 이 없을 때, 동작하지 않는다. 따라서 설정 필수
		// earliest 속성은 offset 을 처음으로 설정해서 읽겠다는 의미이다. -> Kafka CLI 에서 봤던 --from-beginning 과 동일
		// latest 속성은 Consumer 가 실행된 이후로 최근에 온 메시지만 읽겠다는 의미이다.
		properties.setProperty("auto.offset.reset", "earliest");

		log.info("I'm a Kafka Consumer!");
		try (
			// create a consumer
			var consumer = new KafkaConsumer<String, String>(properties)
		) {
			// subscribe to a topic
			// Array 로 구독할 토픽을 여러 개 넘길 수 있다.
			consumer.subscribe(Arrays.asList(TOPIC));

			// poll for data
			while (true) {
				log.info("Polling...");
				// Kafka 로부터 데이터를 받는데, 1초간 기다린다. -> 만일, 1초 안에 Kafka 로부터 데이터가 들어오지 않는다면 timeout 메시지를 보낸다. -> Kafka 의 과부하를 방지하기 위함.
				// poll 의 결과로 Collection 이 반환된다.
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				for (var record : records) {
					log.info("Key: " + record.key() + ", Value : " + record.value());
					log.info("Partition: " + record.partition() + ", Offset : " + record.offset());
				}
				// 현재는 메시지를 받은 후 강제로 종료하고 있음.
				// 정상적으로 Consumer 를 종료하지 않았기 때문에 Consumer 를 재실행하면 Joining Group 을 다시 찾는다.
				// 만일 정상적으로 종료하게 된다면 다시 그룹에 참여하는데 드는 시간이 줄어든다.
			}
		}

	}
}
