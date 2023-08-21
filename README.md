## 移动实习过滤与优化用户画像数据

| 版本   | 任务                                 | 时间           |
|------|------------------------------------|--------------|
| V1.0 | 过滤用户的消费金额,过滤用户购买的商品数量              | `2023-08-12` |
| V2.0 | 计算每个用户累计消费的金额                      | `2023-08-17` |
| V2.0 | 计算每个用户累计消费的金额；<br/>计算每个用户最后一次的消费金额 | `2023-08-21` |

## V1.0 

### 1. 任务说明
两个任务并行处理
1. 过滤用户的消费金额
2. 过滤用户购买的商品数量
### 2. 启动方法

- `userPortrait`文件夹下`CDUserPurchaseAnalysis.java`文件

## V2.0
### 1. 任务说明
计算每个用户累计消费的金额
1. `AccumulateAccountBitMap`文件——使用`BitMap`数据类型作为状态保存
2. `AccumulateAccountSet`使用`HashSet`数据类型作为状态保存
### 2. 启动方法
- 依赖编译设置：pom.xml `<mainClass>设置为相应的主函数路径</mainClass>` 比如`com.sunhb.flinklearn.filter.AccumulateAccountSet</mainClass>`
- 打包：maven package 将程序打包成`jar`运行文件
- 运行：`./bin/flink run xxxxx.jar`
- WebUI查看flink运行过程中内存使用情况



## V2.1 
### 1. 任务说明
*  任务一：计算每个用户累计消费的金额
1. `BigAccumulateAccountBitMap`文件——使用`BitMap`数据类型作为状态保存
2. `BigAccumulateAccountSet`——使用`HashSet`数据类型作为状态保存
* 任务二：计算每个用户最后一次的消费金额
1. `TwoTasksBroadCast` 结合了任务一和二，使用了BroadCast广播方法
### 2. 启动方法
- 依赖编译设置：pom.xml `<mainClass>设置为相应的主函数路径</mainClass>` 比如`com.sunhb.flinklearn.filter.AccumulateAccountSet</mainClass>`
- `pom.xml`部分依赖需要更改`<scope>xxx<scope>`,简单处理可以直接去掉这行，jar包可能会稍微大一点
- 主函数的`private static String csvPath`路径变量需要根据自己实际情况调整，数据位置`src/main/resources/static/data`
- 打包：maven package 将程序打包成`jar`运行文件
- 运行：`./bin/flink run xxxxx.jar`
- WebUI查看flink运行过程中内存使用情况

> 注意：使用broadcast广播方法，即datastream使用了broadcast
``` java 
DataStream<Tuple3<Long,Long,Double>> idFilterStream = dataSource.broadcast();
```

> 数据集百度网盘链接：
> 链接: https://pan.baidu.com/s/1oBw2DnH5nHtUTLhoEqnRfw 提取码: 77es