package per.owisho.learn.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(5000);

        SingleOutputStreamOperator<EventBean> socketStream = env.socketTextStream("localhost", 7777)
                .map(value -> {
                    String[] arr = value.split(",");
                    return new EventBean(arr[0], Integer.parseInt(arr[1]), Integer.parseInt(arr[2]));
                });
        //使用自定义水位线生成窗口
//        SingleOutputStreamOperator<EventBean> watermarkStream = socketStream
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .forGenerator((WatermarkGeneratorSupplier<EventBean>) context -> new MyWatermarkGenerator())
//                        .withTimestampAssigner(TimestampAssignerSupplier.of((SerializableTimestampAssigner<EventBean>) (element, recordTimestamp) -> {
//                            System.out.println("调用extractTimestamp，recordTimestamp=" + recordTimestamp);
//                            return element.getTs() * 1000;
//                        })));
        //使用连续流窗口
        SingleOutputStreamOperator<EventBean> watermarkStream = socketStream
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<EventBean>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(TimestampAssignerSupplier.of((SerializableTimestampAssigner<EventBean>) (element, recordTimestamp) -> element.getTs()*1000)));
        KeyedStream<EventBean, String> keyedStream = watermarkStream.keyBy((KeySelector<EventBean, String>) EventBean::getId);

        SingleOutputStreamOperator<EventBean> sumStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10))).sum("value");
        sumStream.print("sum result=");

        env.execute();
    }

}
/**
 * 1.linux 服务使用命令：nc -lk 7777 可以开启socket进程
 * 2.使用TumblingEventTimeWindows时必须在WatermarkStrategy类中使用withTimestampAssigner明确指定事件时间戳字段，否则会因为获取不到事件时间戳报错
 * 3.withTimestampAssigner 接口时间类的返回的时间单位是毫秒
 * 4.watermark是批量生成的，可以使用env.getConfig().setAutoWatermarkInterval(xx毫秒)修改，默认200ms不建议修改
 */
