package com.sunhb.flinklearn.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: SunHB
 * @createTime: 2023/08/16 下午2:17
 * @description: 使用HashMap作为记录状态的数据结构
 */
public class AccumulateAccountSet {
    private static String csvPath = "/home/root1/sunhb/selflearn/FlinkLearn/src/main/java/com/sunhb/flinklearn/data/CDNOW_master.csv";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> dataSource = env.readTextFile(csvPath).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !s.startsWith("user");
            }
        });
        /**
         * 消费记录的Id,以行号为标注
         */
        AtomicLong lineNumber = new AtomicLong(0);
        /**
         * 计算每个用户累计消费金额
         */
        final Long[] orderId = {0L};
        DataStream<Tuple3<Long,Long,Double>> idFilterStream = dataSource.map(line -> {
            String[] fields = line.split(",");
            String id = fields[0];
            String order_amount = fields[3];
            long orderId_ = lineNumber.incrementAndGet();
            return new Tuple3<Long,Long,Double>(Long.parseLong(id), orderId_,Double.parseDouble(order_amount));
        }).returns(Types.TUPLE(Types.LONG,Types.LONG,Types.DOUBLE));
        //idFilterStream.print();

        //StateTtlConfig ttlConfig = StateTtlConfig
        //        .newBuilder(Time.days(2))
        //        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        //        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        //        .build();


        ValueStateDescriptor<HashSet<Long>> hashSetDescriptor = new ValueStateDescriptor(
                "HashSet",
                TypeInformation.of(new TypeHint<HashSet<Long>>() {
                }));
        ValueStateDescriptor<Double> priceDescriptor = new ValueStateDescriptor<>(
                "AccountPrice",
                Double.class
        );
        idFilterStream.keyBy(i->i.f0)
                        .process(new ProcessFunction<Tuple3<Long,Long,Double>, Object>() {

                            private transient ValueState<HashSet<Long>> hashSetValueState;
                            private transient ValueState<Double> priceState;


                            @Override
                            public void open(Configuration parameters) {
                                hashSetValueState = getRuntimeContext().getState(hashSetDescriptor);
                                priceState = getRuntimeContext().getState(priceDescriptor);
                            }
                            @Override
                            public void processElement(Tuple3<Long,Long,Double> value, Context context, Collector<Object> collector) throws Exception {
                                HashSet<Long> hashSet = hashSetValueState.value();

                                Double price = priceState.value();
                                if(price == null){
                                    price = 0.0;
                                }
                                Long userId = value.f0;
                                Long orderId = value.f1;
                                Double account = value.f2;
                                if( hashSet == null){
                                    hashSet = new HashSet<>();
                                    //hashSet.add(orderId);
                                }
                                if(!hashSet.contains(orderId)){
                                    price += account;
                                    hashSet.add(orderId);
                                    hashSetValueState.update(hashSet);
                                    priceState.update(price);
                                }
                                collector.collect(new Tuple2<>(userId,price));
                            }
                        }).print();

        env.execute("User Accumulate Account HashSet Method");


    }
}
