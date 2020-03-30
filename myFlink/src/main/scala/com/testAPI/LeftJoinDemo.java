package com.testAPI;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.ArrayList;

/**
 * @Classname LeftJoinDemo
 * @Description TODO
 * @Date 2020/3/4 12:07
 * @Created by lz
 */
public class LeftJoinDemo {
    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置使用 EventTime 作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置并行度为1,此处仅用作测试使用。因为 Kafka 为并行数据流，数据只有全部填满分区才会触发窗口操作。
        env.setParallelism(1);

        //ParameterTool parameters = ParameterTool.fromPropertiesFile("配置文件路径");

//        DataStream<String> leftSource = FlinkUtils.createKafkaStream(parameters,"topicA","groupA", SimpleStringSchema.class);
//        DataStream<String> rightSource = FlinkUtils.createKafkaStream(parameters,"topicB","groupB", SimpleStringSchema.class);

        final DataStreamSource<String> leftSource = env.fromElements("a,1001,1000000050000", "a,1002,1000000050000","c,1002,1000000050000");
        final DataStreamSource<String> rightSource = env.fromElements("a,BeiJing,1000000056000", "a,shanghai,1000000056000","b,yantai,1000000056000");


        //左流,设置水位线 WaterMark
        SingleOutputStreamOperator<String> leftStream = leftSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.milliseconds(0)) {
            @Override
            public long extractTimestamp(String s) {
                String[] split = s.split(",");
                return Long.parseLong(split[2]);
            }
        });

        SingleOutputStreamOperator<String> rightStream = rightSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.milliseconds(0)) {
            @Override
            public long extractTimestamp(String s) {
                String[] split = s.split(",");
                return Long.parseLong(split[2]);
            }
        });

        // coGroup() 方法实现 left join 操作
        DataStream<Tuple5<String, String, String, String, String>> joinDataStream = leftStream
                .coGroup(rightStream)
                .where(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        String[] split = value.split(",");
                        return split[0];
                    }
                })
                .equalTo(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        String[] split = value.split(",");
                        return split[0];
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<String, String, Tuple5<String, String, String, String, String>>() {
                    //重写 coGroup() 方法，来实现 left join 功能。
                    @Override
                    public void coGroup(Iterable<String> leftElement, Iterable<String> rightElement, Collector<Tuple5<String, String, String, String, String>> out) throws Exception {
                        System.out.println("=====coGroup执行=================");
                        System.out.println("left:"+leftElement.toString());
                        System.out.println("right:"+rightElement.toString());

                        boolean hasElement = false;
                        //leftElement为左流中的数据
                        for (String leftStr : leftElement) {
                            String[] left = leftStr.split(",");
                            //如果 左边的流 join 上右边的流,rightStream 就不能为空
                            for (String rightStr : rightElement) {
                                String[] right = rightStr.split(",");
                                //将 join 的数据输出
                                out.collect(Tuple5.of(left[0], left[1], right[1], left[2], right[2]));
                                hasElement = true;
                            }
                            if (!hasElement) {
                                out.collect(Tuple5.of(left[0], left[1], "null", left[2], "null"));
                            }
                        }
                    }
                });

        joinDataStream.print();

        env.execute("LeftJoinDemo");
    }
}