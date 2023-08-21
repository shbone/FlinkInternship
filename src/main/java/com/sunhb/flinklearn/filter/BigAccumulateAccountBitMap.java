package com.sunhb.flinklearn.filter;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
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

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author: SunHB
 * @createTime: 2023/08/16 下午2:17
 * @description:
 */
public class BigAccumulateAccountBitMap {
    private static String csvPath = "/home/root1/sunhb/selflearn/FlinkLearn/src/main/resources/static/data/flink_data_1kw.csv";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(15);
        DataStream<String> dataSource = env.readTextFile(csvPath).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !s.startsWith("user");
            }
        });
        /**
         * 计算每个用户累计消费金额
         */

        DataStream<Tuple3<Long,Long,Double>> idFilterStream = dataSource.map(line -> {
            String[] fields = line.split(",");
            String id = fields[1];
            String order_amount = fields[4];
            String uid = fields[0];
            return new Tuple3<Long,Long,Double>(Long.parseLong(id), Long.parseLong(uid),Double.parseDouble(order_amount));
        }).returns(Types.TUPLE(Types.LONG,Types.LONG,Types.DOUBLE)).broadcast();
        //idFilterStream.print();

        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.days(2))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        //create StateDescriptor
        ValueStateDescriptor<Roaring64Bitmap> bitmapDescriptor = new ValueStateDescriptor(
                "Roaring64Bitmap",
                TypeInformation.of(new TypeHint<Roaring64Bitmap>() {
                }));
        ValueStateDescriptor<Double> priceDescriptor = new ValueStateDescriptor<>(
                "AccountPrice",
                Double.class
        );
        idFilterStream.keyBy(i->i.f0)
                        .process(new ProcessFunction<Tuple3<Long,Long,Double>, Object>() {
                            private transient ValueState<Roaring64Bitmap> bitmapState;
                            private transient ValueState<Double> priceState;


                            @Override
                            public void open(Configuration parameters) {
                                bitmapState = getRuntimeContext().getState(bitmapDescriptor);
                                priceState = getRuntimeContext().getState(priceDescriptor);
                            }
                            @Override
                            public void processElement(Tuple3<Long,Long,Double> value, Context context, Collector<Object> collector) throws Exception {
                                Roaring64Bitmap roaring64Bitmap = bitmapState.value();
                                if( roaring64Bitmap == null){
                                    roaring64Bitmap = new Roaring64Bitmap();
                                }
                                Double price = priceState.value();
                                if(price == null){
                                    price = 0.0;
                                }
                                Long userId = value.f0;
                                Long orderId = value.f1;
                                Double account = value.f2;
                                if(!roaring64Bitmap.contains(orderId)){
                                    price += account;
                                    roaring64Bitmap.addLong(orderId);
                                    bitmapState.update(roaring64Bitmap);
                                    priceState.update(price);
                                }
                                collector.collect(new Tuple2<>(userId,price));
                            }
                        }).name("Accumulate Account Consumption").print();

        env.execute("BitMap Filter And Accumulate User Accumulate Account");


    }
}
