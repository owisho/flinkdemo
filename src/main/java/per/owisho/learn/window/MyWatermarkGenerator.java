package per.owisho.learn.window;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

public class MyWatermarkGenerator implements WatermarkGenerator<EventBean> {

    private long ts;

    @Override
    public void onEvent(EventBean event, long eventTimestamp, WatermarkOutput output) {
        System.out.println("调用onEvent, event=" + event + ", eventTimeStamp=" + eventTimestamp + ", ts=" + ts);
        ts = Math.max(ts, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        System.out.println("调用onPeriodicEmit ts=" + ts);
        output.emitWatermark(new Watermark(ts));
    }
}
