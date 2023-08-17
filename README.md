## 移动实习过滤与优化用户画像数据

| 版本   | 任务                    | 时间           |
|------|-----------------------|--------------|
| V1.0 | 过滤用户的消费金额,过滤用户购买的商品数量 | `2023-08-12` |
| V2.0 | 计算每个用户累计消费的金额         | `2023-08-17` |

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
