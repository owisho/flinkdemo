package per.owisho.learn.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class FileSinkDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointStorage("file:///Users/owisho/Desktop/tmp/checkpoints");//注意：使用fileSink时必须开启checkpoint
        env.enableCheckpointing(1000, CheckpointingMode.EXACTLY_ONCE);
        DataGeneratorSource<String> genSource = new DataGeneratorSource<>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return value.toString();
            }
        }, Long.MAX_VALUE, RateLimiterStrategy.perSecond(10), TypeInformation.of(String.class));

        DataStreamSource<String> genStream = env.fromSource(genSource, WatermarkStrategy.noWatermarks(), "genSource");
        FileSink<String> fileSink = FileSink
                .<String>forRowFormat(new Path("/Users/owisho/Desktop/tmp"),
                        new SimpleStringEncoder<String>())
                .withOutputFileConfig(new OutputFileConfig("learn", ".log"))
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd HH"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withRolloverInterval(Duration.ofSeconds(10))
                        .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                        .build()).build();
        genStream.sinkTo(fileSink);
        env.execute();

    }

}
