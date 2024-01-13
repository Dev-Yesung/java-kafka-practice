package com.udemy.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

	private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

	public static void main(String[] args) {
		log.info("Hello, Java-Kafka Practice!");
		// create Producer Properties
		// 자바에서 제공해주는 Properties 클래스로 설정 가능 -> 내부에 HashTable로 구현되어 있음
		var properties = new Properties();
		// connect to localhost
		properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
		// set key/value serializer
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", StringSerializer.class.getName());
		// create the Producer
		try (
			// auto closable 을 구현하고 있기 때문에
			// try with resources 를 이용하면 finally 문에
			// producer.close();를 할 필요 없음
			var producer = new KafkaProducer<String, String>(properties)
		) {
			// create a Producer Record -> 카프카에 보낼 레코드(데이터)를 의미
			// topic, [key], value 를 파라미터로 전달할 수 있다.
			// asynchronous 한 방식의 데이터 전송
			var producerRecord = new ProducerRecord<String, String>("demo_java", "hello world");

			// send data
			producer.send(producerRecord);

			// flush and close the producer
			// producer 에게 모든 데이터를 전송하고 작업이 완료될 때까지 Block 하라는 명령
			// synchronous 방식의 flush -> 실제 프로그램에서 flush를 호출하는 일은 매우 드물다
			producer.flush();
		}
	}

}
