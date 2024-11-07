package per.owisho.learn.window.function;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ProcessWindowFunctionTest extends ProcessWindowFunction<String, String, Integer, TimeWindow> {
    @Override
    public void process(Integer integer, ProcessWindowFunction<String, String, Integer, TimeWindow>.Context context,
                        Iterable<String> elements, Collector<String> out) throws Exception {

    }
}
