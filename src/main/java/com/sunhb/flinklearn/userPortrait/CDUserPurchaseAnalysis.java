package com.sunhb.flinklearn.userPortrait;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;

import org.apache.flink.api.common.typeinfo.Types;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.common.operators.Order.DESCENDING;

/**
 * @author: SunHB
 * @createTime: 2023/08/11 上午9:15
 * @description: *  对用户消费记录进行过滤的目标可能包括：
 * *
 * * 1. 根据时间范围过滤：根据用户指定的起始时间和结束时间，过滤出在该时间范围内的消费记录。
 * *
 * * 2. 根据消费金额过滤：根据用户指定的最小金额和最大金额，过滤出在该金额范围内的消费记录。
 * *
 * * 3. 按消费金额过滤：根据消费金额进行过滤，可以筛选出高额或低额消费的用户或交易，有助于针对不同消费水平的用户制定个性化的营销策略。
 * *
 * * 4. 按购买的商品数过滤：根据购买的商品数量进行过滤，可以筛选出单次购买较多或较少商品的用户，有助于分析用户的购买习惯和消费行为。
 * *
 */

public class CDUserPurchaseAnalysis {

    private static final String csvPath = "/home/root1/sunhb/selflearn/FlinkLearn/src/main/java/com/sunhb/flinklearn/data/CDNOW_master.csv";


    private double Threshold = 50.0;
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> dataSource = env.readTextFile(csvPath).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !s.startsWith("user");
            }
        });

        /*
        处理数据
         */
        //任务一 根据时间范围过滤

        //任务二 根据消费金额过滤：返回消费金额的方位，即最大和最小消费金额
        DataStream<Tuple2<String, Double>> userNameAmountMap = dataSource.map(line -> {
            String[] fields = line.split(",");
            String id = fields[0];
            Double amount = Double.parseDouble(fields[3]);
            return new Tuple2<>(id, amount);
        }).returns(Types.TUPLE(Types.STRING, Types.DOUBLE));
        //DataStream<Tuple3<String, Double, Double>> userConsumptionMinMax = userNameAmountMap
        //        .keyBy(i->i.f0)
        //        .flatMap(new UserComsumption())
        //        .returns(Types.TUPLE(Types.STRING, Types.DOUBLE, Types.DOUBLE));

        //2.1 过滤消费金额起伏
        DataStream<Tuple2<String, Double>> userConsumptionExceeded = userNameAmountMap
                .keyBy(i->i.f0)
                .process(new UserConsumptionExceeded(20.0));
                //.returns(Types.TUPLE(String,Double));


        //任务三


        //任务四 按购买的商品数过滤
        DataStream<Tuple2<String, Integer>> userProductsNum = dataSource.map(line -> {
            String[] fields = line.split(",");
            String id = fields[0];
            Integer products = Integer.parseInt(fields[2]);
            return new Tuple2<>(id, products);
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tuple -> tuple.f0)
                .sum(1);

        /**
         * 写入数据
         */

        String output_path = "./output/twotasks";
        SimpleStringEncoder<String> encoder = new SimpleStringEncoder<>("UTF-8");
        StreamingFileSink<String> streamingFileSink = StreamingFileSink.forRowFormat(new Path(output_path), encoder)
                .build();
        userProductsNum.map(tuple -> tuple.toString()).addSink(streamingFileSink);
        //userConsumptionMinMax.map(tuple -> tuple.toString()).addSink(streamingFileSink);
        userConsumptionExceeded.map(tuple -> tuple.toString()).addSink(streamingFileSink);
        userConsumptionExceeded.print();
        //userProductsNum.print();
        env.execute("User Consumption Analysis!");
    }
}


class UserComsumption extends RichFlatMapFunction<Tuple2<String, Double>, Tuple3<String, Double, Double>> {
    private static Map<String, Tuple2<Double, Double>> map = new HashMap<>();
    @Override
    public void flatMap(Tuple2<String, Double> userNameAmountMap, Collector<Tuple3<String, Double, Double>> collector) throws Exception {

        String id = userNameAmountMap.f0;
        Double consumption = userNameAmountMap.f1;
        if (map.containsKey(id)) {
            Tuple2<Double, Double> doubleTuple2 = map.get(id);
            Double max = Math.max(doubleTuple2.f0, consumption);
            Double min = Math.min(doubleTuple2.f1, consumption);
            map.put(id, new Tuple2<>(max, min));
        } else {
            map.put(id, new Tuple2<>(consumption, consumption));
        }
        Tuple2<Double, Double> userConsumption = map.get(id);
        collector.collect(new Tuple3<>(id, userConsumption.f0, userConsumption.f1));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }
}