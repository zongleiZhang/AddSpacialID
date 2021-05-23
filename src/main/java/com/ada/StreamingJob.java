package com.ada;

import com.ada.common.Constants;
import com.ada.flinkFunction.DensityPF;
import com.ada.flinkFunction.GlobalTreePF;
import com.ada.flinkFunction.MyTimeAndWater;
import com.ada.geometry.TrackPoint;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env;
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        env.readTextFile(Constants.inPutPath)
                .map(TrackPoint::new)
                .assignTimestampsAndWatermarks(new MyTimeAndWater())
                .timeWindowAll(Time.milliseconds(Constants.windowSize))
                .process(new DensityPF())
                .keyBy(value -> Constants.globalSubTaskKeyMap.get(value.key))
                .timeWindow(Time.milliseconds(Constants.windowSize))
                .process(new GlobalTreePF())
                .setParallelism(Constants.globalPartition)
                .print()
        ;
        env.execute("job");
    }
}
