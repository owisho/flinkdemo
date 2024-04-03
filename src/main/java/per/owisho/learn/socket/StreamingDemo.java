package per.owisho.learn.socket;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamingDemo {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        //2.读取数据
        DataStreamSource<String> ds = env.socketTextStream("192.168.88.132", 7777);
        SingleOutputStreamOperator<Tuple2<String, Integer>> os = ds.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    Tuple2<String, Integer> t = Tuple2.of(s1, 1);
                    out.collect(t);
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> ks = os.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //3.切分 转换 分组 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = ks.sum(1);
        //4.输出
        sum.print();
        //5.启动执行
        env.execute();

    }
}
