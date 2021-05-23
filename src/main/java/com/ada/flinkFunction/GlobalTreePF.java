package com.ada.flinkFunction;

import com.ada.GQ_tree.GNode;
import com.ada.GQ_tree.GTree;
import com.ada.geometry.TrackPoint;
import com.ada.model.Density;
import com.ada.model.DensityToGlobalElem;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;


public class GlobalTreePF extends ProcessWindowFunction<DensityToGlobalElem, Tuple2<Integer, TrackPoint>, Integer, TimeWindow> {
    private GTree globalTree;

    public  GlobalTreePF(){ }

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<DensityToGlobalElem> elements,
                        Collector<Tuple2<Integer, TrackPoint>> out) {
        //根据Global Index给输入项分区
        int[][] density = processElemAndDensity(elements, out);

        // 调整Global Index, 然后将调整结果同步到相关的Local Index中。
        if (density != null){
            //调整Global Index
            List<Tuple2<GNode, GNode>> tup2s = globalTree.updateTree(density);
            //Global Index发生了调整
            if (!tup2s.isEmpty()){
                globalTree.replaceNode(tup2s);
            }
        }
    }


    private int[][] processElemAndDensity(Iterable<DensityToGlobalElem> elements,
                                          Collector<Tuple2<Integer, TrackPoint>> out) {
        int[][] result = null;
        for (DensityToGlobalElem element : elements) {
            if (element.value instanceof TrackPoint){
                TrackPoint point = (TrackPoint) element.value;
                List<Integer> leafs = globalTree.searchLeafNodes(point);
                for (Integer leaf : leafs){
                    out.collect(new Tuple2<>(leaf, point));
                }
            }else {
                result = ((Density) element.value).grids;
            }
        }
        return result;
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        globalTree = new GTree();
    }

}
