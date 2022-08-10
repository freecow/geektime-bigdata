## 题目一: 分析一条 TPCDS SQL

分析一条 TPCDS SQL（请基于 Spark 3.1.1 版本解答）

- 运行该SQL，如q38，并截图该SQL的SQL执行图
- 该SQL用到了哪些优化规则（optimizer rules）
- 请各用不少于200字描述其中的两条优化规则
- SQL从中任意选择一条：https://github.com/apache/spark/tree/master/sql/core/src/test/resources/tpcds



### 1.环境准备

```bash
# 从github下载TPCDS数据生成器
git clone https://github.com/maropu/spark-tpcds-datagen.git

cd spark-tpcds-datagen

# 下载Spark3.1.1到spark-tpcds-datagen目录并解压
wget https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz

tar -zxvf spark-3.1.1-bin-hadoop2.7.tgz
```

生成数据

```bash
# 生成数据
mkdir -p tpcds-data-1g

export SPARK_HOME=./spark-3.1.1-bin-hadoop2.7

./bin/dsdgen --output-location tpcds-data-1g
```

运行生成数据结果示意

![image-20220809172701534](http://pic-bkt.oss-cn-beijing.aliyuncs.com/blog/2022-08-09-092706.png)

### 2.选择执行q38 SQL

```bash
# 下载三个test jar并放到当前目录
wget https://repo1.maven.org/maven2/org/apache/spark/spark-catalyst_2.12/3.1.1/spark-catalyst_2.12-3.1.1-tests.jar

wget https://repo1.maven.org/maven2/org/apache/spark/spark-core_2.12/3.1.1/spark-core_2.12-3.1.1-tests.jar

wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql_2.12/3.1.1/spark-sql_2.12-3.1.1-tests.jar

# 执行SQL
./spark-3.1.1-bin-hadoop2.7/bin/spark-submit \
--conf spark.sql.planChangeLog.level=WARN \
--class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark \
--jars spark-core_2.12-3.1.1-tests.jar,spark-catalyst_2.12-3.1.1-tests.jar \
spark-sql_2.12-3.1.1-tests.jar \
--data-location tpcds-data-1g --query-filter "q38"\
> spark.q38.log 2>&1
```

运行q38结果示意如下，详细输出日志请见本目录下的spark.q38.log

![image-20220810200854214](http://pic-bkt.oss-cn-beijing.aliyuncs.com/blog/2022-08-10-120912.png)

### 3.该SQL采用了哪些优化规则

归纳起来采用了如下优化规则：

org.apache.spark.sql.catalyst.optimizer.ColumnPruning

org.apache.spark.sql.catalyst.optimizer.ReplaceIntersectWithSemiJoin

org.apache.spark.sql.catalyst.optimizer.ReplaceDistinctWithAggregate

org.apache.spark.sql.catalyst.optimizer.ReorderJoin

org.apache.spark.sql.catalyst.optimizer.PushDownPredicates

org.apache.spark.sql.catalyst.optimizer.PushDownLeftSemiAntiJoin

org.apache.spark.sql.catalyst.optimizer.CollapseProject

org.apache.spark.sql.catalyst.optimizer.EliminateLimits

org.apache.spark.sql.catalyst.optimizer.ConstantFolding

org.apache.spark.sql.catalyst.optimizer.RemoveNoopOperators

org.apache.spark.sql.catalyst.optimizer.InferFiltersFromConstraints

org.apache.spark.sql.catalyst.optimizer.RewritePredicateSubquery



### 4.描述其中的两条优化规则

选择PushDownPredicates和ColumnPruning这两条规则。

PushDownPredicates：谓词下推规则，主要是把filter操作尽可能下推到贴近数据源的地方，这样做可以将计算转移至数据源端，减少spark加载和JOIN计算的数据量，在扫描数据的时候就对数据进行了过滤，这样参与计算的数据量就会明显减少，类似JOIN的耗时也会降低。但注意谓词下推只支持确定性的表达式，如果表达式类似first、last、collect_set、collect_list、rand等，filters操作就不能进行下推，因为这些函数是不确定性的。 

ColumnPruning：列裁剪规则，列裁剪在Spark SQL中是由 ColumnPruning实现的，因为我们查询的表往往是由很多个字段组成的，但每次查询我们很大可能是不需要扫描出所有字段的，这个时候就利用列裁剪，把那些查询不需要的字段过滤掉，使得扫描的数据量明显减少。ColumnPruning优化一方面大幅减少了网络、内存的数据量消耗，另一方面对于列存格式（Parquet）来说，有效提高了扫描效率。



## 题目二:架构设计题

你是某互联网公司的大数据平台架构师，请设计一套基于Lambda架构的数据平台架构，要求尽可能多的把课程中涉及的组件添加到该架构图中。并描述Lambda架构的优缺点，要求不少于300字。



### 1.架构图

![lambda2](http://pic-bkt.oss-cn-beijing.aliyuncs.com/blog/2022-08-10-222709.png)

### 2.Lamda架构的优缺点

#### Lamda架构的优点

容错能力：任何数据被损坏，可以删除从损坏点以来的所有数据，并替换为正确的数据。如果数据统计口径发生变化，可以通过重新运行离线任务，很快对数据修正纠错；

可扩展性：Lambda体系架构的实现大多是分布式系统构建的，而分布式系统基本上都是支持水平扩展的，因此，Lambda 架构是具有较强的可扩展性；

可调试性：Lambda体系架构简单，通过每一层的输入和输出，极大地简化了计算和查询的调试；

#### Lamda架构的缺点

资源需求大：涉及数据量较大，且两系统可能同时需要占用资源，对计算资源和存储资源占用较多；

部署复杂：需要部署离线及实时计算两套系统，给运维造成的负担比较重；

两套计算代码：业务需要根据需求，开发离线及实时两套代码，且分开代码维护，需求变更涉及到修改和测试两套代码，任务量大；
