package com.ada.common;

import com.ada.geometry.*;

import java.io.FileInputStream;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.*;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.assignKeyToParallelOperator;

public class Constants implements Serializable {

    /**
     * 定义Double类型的零
     */
    public final static double zero = 0.00001;

    public final static DecimalFormat df = new DecimalFormat("#.00000");


    public static String inPutPath;

    public static String outPutPath;

    /**
     * 输入的并行度
     */
    public static int inputPartition;

    /**
     * 全局索引的并行度
     */
    public static int globalPartition;

    /**
     * 从节点的并行度
     */
    public static int dividePartition;

    /**
     * 网格密度
     */
    public static int gridDensity;

    public static long windowSize;

    public static int logicWindow;

    public static Rectangle globalRegion;

    /**
     * subTask: globalSubTask
     * value: key
     */
    public static Map<Integer,Integer> globalSubTaskKeyMap = new HashMap<>();

    static {
        try {
            Properties pro = new Properties();
            FileInputStream in = new FileInputStream("conf.properties");
            pro.load(in);
            in.close();
            inputPartition = Integer.parseInt(pro.getProperty("inputPartition"));
            globalPartition = Integer.parseInt(pro.getProperty("globalPartition"));
            dividePartition = Integer.parseInt(pro.getProperty("dividePartition"));
            windowSize = Integer.parseInt(pro.getProperty("windowSize"));
            logicWindow = Integer.parseInt(pro.getProperty("logicWindow"));
            gridDensity = Integer.parseInt(pro.getProperty("gridDensity"));

            if ("TAXI-BJ".equals(pro.getProperty("dataSet"))){
                globalRegion = new Rectangle(new Point(0.0,0.0), new Point(1929725.6050, 1828070.4620));
                if ("Windows 10".equals(System.getProperty("os.name"))){
                    inPutPath = "D:\\研究生资料\\track_data\\北京出租车\\merge\\Experiment\\convert";
                    outPutPath = "D:\\研究生资料\\track_data\\北京出租车\\merge\\Experiment\\Result\\";
                }else {
                    inPutPath = "/home/chenliang/data/zzl/TAXI-BJ/convert";
                    outPutPath = "/home/chenliang/data/zzl/TAXI-BJ/Result/";
                }
            }else {
                globalRegion = new Rectangle(new Point(0.0,0.0), new Point(8626.0,8872.0));
                if ("Windows 10".equals(System.getProperty("os.name"))){
                    inPutPath = "D:\\研究生资料\\track_data\\成都滴滴\\Sorted_2D\\XY_20161101";
                    outPutPath = "D:\\研究生资料\\track_data\\成都滴滴\\Experiment\\Result\\";
                }else {
                    inPutPath = "/home/chenliang/data/zzl/DIDI-CD/Single";
                    outPutPath = "/home/chenliang/data/zzl/DIDI-CD/Result/";
                }
            }

            /*
             * 86-- 128是 256
             */
            int maxParallelism = 128;
//        int maxParallelism = 256;
            Set<Integer> usedSubtask = new HashSet<>();
            for (int i = 0; i < 1000000; i++) {
                Integer subTask = assignKeyToParallelOperator(i, maxParallelism, globalPartition);
                if (!usedSubtask.contains(subTask)) {
                    usedSubtask.add(subTask);
                    globalSubTaskKeyMap.put(subTask, i);
                    if (usedSubtask.size() == globalPartition)
                        break;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public static boolean isEqual(double a, double b){
        return Math.abs(a - b) < zero;
    }

}
