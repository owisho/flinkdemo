package per.owisho.learn.metrics;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MetricsDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataGeneratorSource<String> genSource = new DataGeneratorSource<String>(new GeneratorFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return value / 3 + "";
            }
        }, Long.MAX_VALUE, RateLimiterStrategy.perSecond(2), Types.STRING);

        SingleOutputStreamOperator<String> stream = env.fromSource(genSource, WatermarkStrategy.noWatermarks(), "gen-source").map(new MyMapper());

        KeyedStream<String, String> keyedStream = stream.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String value) throws Exception {
                return value;
            }
        });

        SingleOutputStreamOperator<String> reduceStream = keyedStream.reduce(new ReduceFunction<String>() {
            @Override
            public String reduce(String value1, String value2) throws Exception {
                System.out.println("value1=" + value1);
                System.out.println("value2=" + value2);
                return value1;
            }
        });

        reduceStream.print();
        env.execute("metrics_demo_new");

    }
}
