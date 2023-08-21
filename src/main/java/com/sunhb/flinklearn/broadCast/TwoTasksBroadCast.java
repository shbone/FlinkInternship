package com.sunhb.flinklearn.broadCast;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

/**
 * @author: SunHB
 * @createTime: 2023/08/20 下午3:11
 * @description:
 */
public class TwoTasksBroadCast {

    private static String csvPath = "/home/root1/sunhb/selflearn/FlinkLearn/src/main/resources/static/data/flink_data_1kw.csv";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        DataStream<String> dataSource = env.readTextFile(csvPath).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !s.startsWith("user");
            }
        });

        DataStream<Tuple3<Long, Long, Double>> idFilterStream = dataSource.map(line -> {
            String[] fields = line.split(",");
            String id = fields[1];
            String order_amount = fields[4];
            String uid = fields[0];
            return new Tuple3<Long, Long, Double>(Long.parseLong(id), Long.parseLong(uid), Double.parseDouble(order_amount));
        }).returns(Types.TUPLE(Types.LONG, Types.LONG, Types.DOUBLE));
        //idFilterStream.print();


        /**
         * 任务1：过滤每个用户最后一次消费的金额
         */
        ValueStateDescriptor<Double> useridDescriptor = new ValueStateDescriptor<Double>(
                "MaxPrice",
                Double.class
        );
        idFilterStream.keyBy(i -> i.f0).flatMap(new RichFlatMapFunction<Tuple3<Long, Long, Double>, Tuple2<Long,Double>>() {
            private ValueState<Double> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(useridDescriptor);
            }


            @Override
            public void flatMap(Tuple3<Long, Long, Double> value, Collector<Tuple2<Long,Double>> collector) throws Exception {
                Long userId = value.f0;
                Double price = value.f2;
                Double aDouble = valueState.value();
                if(aDouble == null){
                    aDouble = 0.0;
                }
                if(price > aDouble){
                    valueState.update(price);
                    collector.collect(new Tuple2<Long,Double>(userId,price));
                }
            }
        }).name("User last Consumption ").print();
        /**
         * 任务2：计算每个用户累计消费金额
         */
        //create StateDescriptor
        ValueStateDescriptor<Roaring64Bitmap> bitmapDescriptor = new ValueStateDescriptor(
                "Roaring64Bitmap",
                TypeInformation.of(new TypeHint<Roaring64Bitmap>() {
                }));
        ValueStateDescriptor<Double> priceDescriptor = new ValueStateDescriptor<>(
                "AccountPrice",
                Double.class
        );
        idFilterStream.keyBy(i -> i.f0)
                .process(new ProcessFunction<Tuple3<Long, Long, Double>, Object>() {
                    private transient ValueState<Roaring64Bitmap> bitmapState;
                    private transient ValueState<Double> priceState;


                    @Override
                    public void open(Configuration parameters) {
                        bitmapState = getRuntimeContext().getState(bitmapDescriptor);
                        priceState = getRuntimeContext().getState(priceDescriptor);
                    }

                    @Override
                    public void processElement(Tuple3<Long, Long, Double> value, Context context, Collector<Object> collector) throws Exception {
                        Roaring64Bitmap roaring64Bitmap = bitmapState.value();
                        if (roaring64Bitmap == null) {
                            roaring64Bitmap = new Roaring64Bitmap();
                        }
                        Double price = priceState.value();
                        if (price == null) {
                            price = 0.0;
                        }
                        Long userId = value.f0;
                        Long orderId = value.f1;
                        Double account = value.f2;
                        if (!roaring64Bitmap.contains(orderId)) {
                            price += account;
                            roaring64Bitmap.addLong(orderId);
                            bitmapState.update(roaring64Bitmap);
                            priceState.update(price);
                        }
                        collector.collect(new Tuple2<>(userId, price));
                    }
                }).name("Accumulated Amount Task").print();
        //env.fromElements(1,2)
        env.execute("Two Tasks BroadCast");
    }
}
