package com.testAPI;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @Classname CounterTest
 * @Description TODO
 * @Date 2020/3/4 16:02
 * @Created by lz
 */
    public class CounterTest {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);//10秒一个checkpoint
        env.setStateBackend(new FsStateBackend("file:///root/checkpoint/flink"));
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);//指定处理的时间特性
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(50)));//重启策略RestartStrategies.fixedDelayRestart(3,Time.seconds(100))，会在100秒内重试3次，失失败事务中到开始到错误数据段的数据
        //env.setRestartStrategy(RestartStrategies.fallBackRestart())//重启策略 RestartStrategies.fallBackRestart()失败后立即重启，丢失失败事务中到开始到错误数据段的数据
        //env.setRestartStrategy(RestartStrategies.noRestart())//重启策略 RestartStrategies.fallBackRestart()失败后立即重启，丢失失败事务中到开始到错误数据段的数据
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);//确保一次语义
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);//退出不删除checkpoint
        // prevent the tasks from failing if an error happens in their checkpointing, the checkpoint will just be declined.
        env.getCheckpointConfig().setFailOnCheckpointingErrors(true);

        env.setParallelism(5);
//        //--hostname 10.24.14.193  --port 9000
//        final ParameterTool params = ParameterTool.fromArgs(args);
//        String hostname = params.has("hostname") ? params.get("hostname") : "localhost";
//        int port = params.has("port") ? params.getInt("port") : 9000;
//
//        System.out.println("hostName=" + hostname + " port=" + port);
        Properties properties   = new Properties();
        properties.setProperty("bootstrap.servers", "mydis:9092");
        properties.setProperty("group.id", "consumer-group11");
        // 设置隔离级别
        properties.setProperty("isolation.level","read_committed");
        // 关闭自动提交
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("session.timeout.ms", "30000");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //properties.setProperty("auto.offset.reset", "latest");
         DataStreamSource<String> kafkasource = env.addSource(new FlinkKafkaConsumer011<String>("wordcounter", new SimpleStringSchema(), properties));

        //数据来源

        //operate
        kafkasource.map(new RichMapFunction<String, String>() {

            //第一步：定义累加器
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                //第二步：注册累加器
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public String map(String s) throws Exception {
                //第三步：累加
                this.numLines.add(1);
                return s;
            }
        });

        //数据去向
        kafkasource.print();

        //执行
        JobExecutionResult kafkaResult = env.execute("socketTest");

        //第四步：结束后输出总量；如果不需要结束后持久化，可以省去，因为在flinkUI中可以看到
        String total = kafkaResult.getAccumulatorResult("num-lines").toString();
        System.out.println(total);
    }
}