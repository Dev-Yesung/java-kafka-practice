package com.udemy.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithGroup {

	private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithGroup.class.getSimpleName());
	private static final String GROUP_ID = "my-java-application";
	private static final String TOPIC = "demo_java";

	/*
		Intellij 애플리케이션 설정에서 multiple instance 를 허용하도록 한다.
		여러 Consumer 가 토픽에 참여하면 파티션이 분할되어 할당된다.
		이렇게 컨슈머가 그룹에 들어오거나 나갈 때, 파티션이 이동하는 걸 리밸런싱이라 한다.
		또한 새 파티션을 토픽에 추가할 때도 발생한다.
		리밸런싱 전략에 따라 컨슈머에게 할당되는 파티션이 달라진다.

		큰 틀에서 파티션 할당 방식
		1. Eager Rebalance
		-> 모든 컨슈머의 읽기가 중단되고 할당된 파티션들이 모두 해제된 후 모든 파티션이 파티션에 다시 밸런싱 된다.
		-> 이때 전체 컨슈머 그룹이 작동을 멈추게 되고(Stop-The-World) 기존의 컨슈머가 원래의 파티션으로 돌아간다는 보장이 없다.
		-> 원치않는 결과를 낳을 수 있음

		2. Cooperative Rebalance(Incremental Rebalance)
		-> 모든 파티션을 모든 컨슈머에게 재할당하는 게 아니라
		-> 파티션을 작은 그룹으로 나눠서 일부만 한 컨슈머에서 다른 컨슈머로 할당한다.
		-> 이렇게 함으로써 기존에 다른 컨슈머들도 중단 없이 데이터를 처리할 수 있게 된다.
		-> 이 과정이 여러 번 반복되어 안정적으로 할당 받을 수 있다.

		할당 전략은 'partition.assignment.strategy' 속성을 통해 설정할 수 있다.

		구체적인 할당 전략
		[Eager Strategy]
		컨슈머 그룹을 해체하고 다시 만든다. stop-the-world가 발생하고, 그룹이 클수록 오래 걸린다.
		1. RangeAssignor: 토픽을 기준으로 파티션을 할당하는 전략
		-> 토픽 당 기준으로 할당해서 균형을 맞추기 어렵다.

		2. RoundRobin: 라운드-로빈 방식으로 모든 파티션을 모든 토픽에 걸쳐 할당한다.
		-> 공평하게 분배하기 좋음. 모든 컨슈머가 같은 수의 파티션을 갖는다.

		3. StickyAssigner: 라운드-로빈처럼 밸런싱을 하고 컨슈머 수에 변동이 생기면 파티션의 움직임을 최소화 한다.

		[Cooperative Strategy]
		CooperativeStickyAssigner: 기존의 StickyAssigner 에 Cooperative 방식을 추가

		하지만 Kafka 3.0의 Strategy Assigner 는 List 로 되어 있다.
		[RangeAssignor, CooperativeStickyAssignor] 가 기본값
		RangeAssignor 가 기본값으로 사용되다가 전략에서 지우면 CooperativeStickyAssigner 로 실행된다.

		Kafka Connect 를 사용하면 Cooperative Rebalance 가 기본값으로 사용된다.
		Kafka Streams 를 사용하면 StreamsPartitionAssignor 가 기본값으로 사용된다.

		** Static Group Membership
		카프카는 기본적으로 컨슈머가 그룹을 떠났을 때 재할당을 한다.
		그래서 나갔던 컨슈머가 재합류 하더라도 새로운 memberID 를 갖기 때문에 새로운 파티션으로 할당한다.
		하지만, 만일 group.instance.id 에 특정 값을 할당하면 컨슈머를 static member 로 만들 수 있다.
		static member 인 컨슈머가 그룹을 떠났을 때, session.timeout.ms 에 설정된 시간 안에 돌아오면
		특정 파티션이 특정 컨슈머에 속하게 되어 리밸런싱이 안일어난다.
		그리고 컨슈머가 로컬 캐시와 로컬 상태를 유지해야한다면 이때 유용하다.
		왜냐하면 컨슈머가 특정 파티션을 갖고 있게해서 캐싱 데이터를 다시 만들지 않아도 되기 때문이다.
		이 기능은 쿠버네티스를 사용할 경우 굉장히 유용하다고 한다.
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
			var consumer = new KafkaConsumer<String, String>(properties)
		) {
			final Thread mainThread = Thread.currentThread();
			Runtime.getRuntime().addShutdownHook(new Thread(() -> {
				log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
				consumer.wakeup();

				try {
					mainThread.join();
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}));

			consumer.subscribe(Arrays.asList(TOPIC));
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
				for (var record : records) {
					log.info("Key: " + record.key() + ", Value : " + record.value());
					log.info("Partition: " + record.partition() + ", Offset : " + record.offset());
				}
			}
		} catch (WakeupException e) {
			log.info("Consumer is starting to shut down...");
		} catch (Exception e) {
			log.error("Unexpected exception in the consumer", e);
		}
	}
}
