package per.owisho.learn.connector;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class KafkaConnectorDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(1000 * 60 * 10);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/owisho/Desktop/tmp/checkpoints");

        KafkaSource<String> source = getKafkaSource();
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
        stream.print();

        env.execute("kafka consume demo");
    }

    public static KafkaSource<String> getKafkaSource() {
        String topics = "test";
        String brokers = "localhost:9092";
        String groupId = "test";
        KafkaSourceBuilder<String> builder = KafkaSource.<String>builder()
                // 订阅 Topic 列表中所有 Partition 的消息
                .setTopics(topics.split(","))
                // kafka 集群节点信息
                .setBootstrapServers(brokers)
                // 设置消费者组 ID
                .setGroupId(groupId)
                // 设置动态分区检查，每 10 秒检查一次新分区
                .setProperty("partition.discovery.interval.ms", "10000")
                //
                .setProperty("max.partition.fetch.bytes", String.valueOf(4 * 1024 * 1024))
                // 设置解析 Kafka 消息的反序列化器
                .setValueOnlyDeserializer(new SimpleStringSchema());

        builder.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST));
//        builder.setStartingOffsets(OffsetsInitializer.earliest());

        KafkaSource<String> source = builder.build();
        return source;
    }

}
