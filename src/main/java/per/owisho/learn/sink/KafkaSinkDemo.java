package per.owisho.learn.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaSinkDemo {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/owisho/Desktop/tmp/checkpoints");//注意：使用fileSink时必须开启checkpoint
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        DataGeneratorSource<String> genSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return value.toString();
            }
        }, Long.MAX_VALUE, RateLimiterStrategy.perSecond(1), TypeInformation.of(String.class));

        DataStreamSource<String> genStream = env.fromSource(genSource, WatermarkStrategy.noWatermarks(), "genSource");

        try {
            env.execute("kafka-sink-demo");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
