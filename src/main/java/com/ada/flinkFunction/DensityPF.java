package com.ada.flinkFunction;

import com.ada.common.Constants;
import com.ada.geometry.TrackPoint;
import com.ada.model.Density;
import com.ada.model.DensityToGlobalElem;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DensityPF extends ProcessAllWindowFunction<TrackPoint, DensityToGlobalElem, TimeWindow> {
    private int[][] grids;
    private boolean isFirst;
    private int key;

    @Override
    public void process(Context context,
                        Iterable<TrackPoint> elements,
                        Collector<DensityToGlobalElem> out){
        for (TrackPoint point : elements) {
            int row = (int) Math.floor(((point.data[0] - Constants.globalRegion.low.data[0])/(Constants.globalRegion.high.data[0] - Constants.globalRegion.low.data[0]))*(Constants.gridDensity+1.0));
            int col = (int) Math.floor(((point.data[1] - Constants.globalRegion.low.data[1])/(Constants.globalRegion.high.data[1] - Constants.globalRegion.low.data[1]))*(Constants.gridDensity+1.0));
            grids[row][col]++;
            out.collect(new DensityToGlobalElem(key, point));
            key = (++key)%Constants.globalPartition;
        }
        if (!isFirst && context.window().getStart()%(Constants.windowSize*Constants.logicWindow*4) == 0){
          for (int i = 0; i < Constants.globalPartition; i++) {
              out.collect(new DensityToGlobalElem(i, new Density(grids)));
          }
          grids = new int[Constants.gridDensity+1][Constants.gridDensity+1];
        }
        isFirst = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        grids = new int[Constants.gridDensity+1][Constants.gridDensity+1];
        isFirst = true;
        key = 0;
    }
}
