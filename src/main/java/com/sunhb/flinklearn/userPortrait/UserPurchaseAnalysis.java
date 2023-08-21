package com.sunhb.flinklearn.userPortrait;

import akka.stream.impl.io.FileSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import static org.apache.flink.api.common.operators.Order.DESCENDING;


/**
 * @author: SunHB
 * @createTime: 2023/08/09 下午9:07
 * @description:用户购买分析，分析购买某个机型的用户是否有偏向性

 */
public class UserPurchaseAnalysis {

    public static void main(String[] args) {
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        String csvPath = "/home/root1/sunhb/selflearn/FlinkLearn/src/main/java/com/sunhb/flinklearn/data/train2.csv";
        DataSet<UserInfo> csvInput =env.readCsvFile(csvPath)
                .ignoreFirstLine() //忽略首行
                .fieldDelimiter(",")
                //.includeFields("1100001101")
                .pojoType(UserInfo.class,"pid","label","gender","age","province","city","make","model");

        /*
        处理数据
         */
        try {
            //按照城市名称拆分
            DataSet<Tuple2<String ,Integer>> map = csvInput.map(new MapFunction<UserInfo, Tuple2<String, Integer>>() {
                @Override
                public Tuple2<String, Integer> map(UserInfo userInfo) throws Exception {
                    String city = userInfo.getCity();
                    return new Tuple2<>(city, 1);
                }
            });
            DataSet<Tuple2<String, Integer>> aggregate = map.groupBy(0).aggregate(Aggregations.SUM, 1)
                    .sortPartition(1,DESCENDING);
            aggregate.print();
            //city.print();

            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        /**
         * 写入数据
         */
        //FileSink.<String>for()

    }
}
