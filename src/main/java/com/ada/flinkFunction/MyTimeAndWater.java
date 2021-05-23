package com.ada.flinkFunction;

import com.ada.geometry.TrackPoint;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class MyTimeAndWater implements AssignerWithPeriodicWatermarks<TrackPoint> {
    private long currentMaxTimestamp = 0;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - 1);
    }

    @Override
    public long extractTimestamp(TrackPoint point, long l) {
        long timestamp = point.timestamp;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}
