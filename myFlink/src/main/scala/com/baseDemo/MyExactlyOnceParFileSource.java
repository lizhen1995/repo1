package com.baseDemo;


import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Iterator;

public class MyExactlyOnceParFileSource extends RichParallelSourceFunction<Tuple2<String, String>> implements CheckpointedFunction {

    private String path;

    private boolean flag = true;

    private long offset = 0;

    private transient ListState<Long> offsetState;

    public MyExactlyOnceParFileSource() {}

    /**
     *
     * @param path   /var/data
     */
    public MyExactlyOnceParFileSource(String path) {
        this.path = path;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
        //获取offsetState历史值
        Iterator<Long> iterator = offsetState.get().iterator();
        while (iterator.hasNext()) {
            offset = iterator.next();
        }

        int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        //var/data/0.txt
        RandomAccessFile randomAccessFile = new RandomAccessFile(path + "/" + subtaskIndex + ".txt", "r");

        //从指定的位置读取数据
        randomAccessFile.seek(offset);

        //获取一个锁
        final Object lock = ctx.getCheckpointLock();

        while (flag) {

            String line = randomAccessFile.readLine();

            if(line != null) {
                line = new String(line.getBytes("ISO-8859-1"), "UTF-8");

                synchronized (lock) {
                    offset = randomAccessFile.getFilePointer();
                    //将数据发送出去
                    ctx.collect(Tuple2.of(subtaskIndex + "", line));
                }
            } else {
                Thread.sleep(1000);
            }
        }


    }

    @Override
    public void cancel() {
        flag = false;
    }

    /**
     * 定期将制定的状态数据保存到StateBackend中
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        //将历史值清除
        offsetState.clear();
        //根据最新的状态值
        //offsetState.update(Collections.singletonList(offset));
        offsetState.add(offset);
    }

    /**
     * 初始化OperatorState，生命周期方法，构造方法执行后会执行一次
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        //定义一个状态描述器
        ListStateDescriptor<Long> stateDescriptor = new ListStateDescriptor<Long>(
                "offset-state",
                //TypeInformation.of(new TypeHint<Long>() {})
                //Long.class
                Types.LONG
        );


        //初始化状态或获取历史状态(OperatorState)
        offsetState = context.getOperatorStateStore() .getListState(stateDescriptor);

    }
}
