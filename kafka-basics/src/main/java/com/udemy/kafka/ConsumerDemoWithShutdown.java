package com.udemy.kafka;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithShutdown {

	private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
	private static final String GROUP_ID = "my-java-application";
	private static final String TOPIC = "demo_java";

	/*
		Consumer 에 shutdown hook 을 추가해서 우아하게 종료할 수 있다.
		1. shutdownHook 을 지정한다.
		2. shutdownHook 에서 Consumer.wakeup()을 실행시킨다
		(poll()수행 후에 WakeupException 이 발생하도록 한다.)
		3. Thread.join()을 해서 메인 스레드가 일을 모두 마칠 때까지 기다리도록 한다.
		4. 메인 스레드에서 poll 을 수행한다.
		5. shutdownHook 에 걸어둔 join()이 끝나고 프로그램이 종료됨
	 */
	public static void main(String[] args) {
		var properties = new Properties();
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", StringDeserializer.class.getName());
		properties.setProperty("group.id", GROUP_ID);
		properties.setProperty("auto.offset.reset", "earliest");

		log.info("I'm a Kafka Consumer!");
		try (
			// Consumer 가 종료될 때 offset 을 커밋하고 종료한다.
			var consumer = new KafkaConsumer<String, String>(properties)
		) {
			// get a reference to the main thread
			final Thread mainThread = Thread.currentThread();
			// adding the shutdown hook
			// 자바 표준 메서드
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
				// 다음과 같이 wakeup() 메서드를 실행한 다음 poll 을 실행할 경우 WakeupException 을 던진다.
				consumer.wakeup();

				try {
					// join the main thread to allow the execution of the code in the main thread
					// 메인 프로그램이 끝날 때까지 훅이 대기한다. -> 스레드 조인
					mainThread.join();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}));

			consumer.subscribe(Arrays.asList(TOPIC));
			while (true) {
				log.info("Polling...");
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				for (var record : records) {
					log.info("Key: " + record.key() + ", Value : " + record.value());
					log.info("Partition: " + record.partition() + ", Offset : " + record.offset());
				}
			}
		} catch (WakeupException e) {
			// WakeupException 이 발생할 경우 로그 출력
			// shutdownHook 에 join()을 걸어놨기 때문에,
			// Thread join 이 이뤄지고 shutdownHook 에 의해 프로그램 종료
			log.info("Consumer is starting to shut down...");
		} catch (Exception e) {
			// 나머지 예외 처리
			log.error("Unexpected exception in the consumer", e);
		}
	}
}
