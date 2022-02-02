import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.slf4j.LoggerFactory
import java.util.Properties

class SimpleStreams {
    companion object {
        const val APPLICATION_NAME = "streams-application" // 스트림즈 애플리이션은 아이디 값을 기준으로 병렬처리 한다.
        const val BOOTSTRAP_SERVERS = "13.124.252.159:9092"
        const val SOURCE_TOPIC = "random.number"
        const val SINK_TOPIC = "over5.number"
    }
}

fun main(args: Array<String>) {
    val props = Properties()
    val logger = LoggerFactory.getLogger(SimpleStreams.javaClass)

    props[StreamsConfig.APPLICATION_ID_CONFIG] = SimpleStreams.APPLICATION_NAME
    props[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = SimpleStreams.BOOTSTRAP_SERVERS
    // 스트림 처리를 위한 메시지 키 직렬화/역직렬화 방식
    props[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String()::class.java
    // 스트림 처리를 위한 메시지 값 직렬화/역직렬화 방식
    props[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String()::class.java

    val builder = StreamsBuilder()
    val stream = builder.stream<String, String>(SimpleStreams.SOURCE_TOPIC)

    // 메시지 값이 5 이상이면 SINK_TOPIC에 저장
    val filteredStream = stream.filter { _, value ->
        logger.info("{}", value)
        Integer.parseInt(value) > 5
    }

    filteredStream.to(SimpleStreams.SINK_TOPIC)

    val streams = KafkaStreams(builder.build(), props)
    streams.start()
}
