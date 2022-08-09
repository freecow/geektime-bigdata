# 题目一: 分析一条 TPCDS SQL

分析一条 TPCDS SQL（请基于 Spark 3.1.1 版本解答）

- 运行该SQL，如q38，并截图该SQL的SQL执行图
- 该SQL用到了哪些优化规则（optimizer rules）
- 请各用不少于200字描述其中的两条优化规则
- SQL从中任意选择一条：https://github.com/apache/spark/tree/master/sql/core/src/test/resources/tpcds

答：


(1) 我选用的是q38这条SQL，SQL执行图请见上图，由于输出日志太长，一张截图放不下，我将日志文件上传到了网上，链接请见https://gitee.com/leoIamOk/geek-university-bigdata-training-camp/raw/master/graduation_assignment/spark.rules.log。

(2) 该 SQL 用到了如下优化规则，

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

(3) 选择 PushDownPredicates 和 ReorderJoin 这两条规则

PushDownPredicates 这个规则通过其他的操作把 filter 操作下推到离数据源更近的地方，这样做可以将计算转移至数据源端，减少 spark 加载和计算的数据量，但不是所有的操作都支持。比如，如果表达式不是确定性的，这就不行，假如我们使用类似 first，last，collect_set,collect_list,rand 等，filters 操作就不能通过这些操作而进行下推，因为这些函数是不确定性的。 



ReorderJoin 这个规则是对 Join 操作进行重新排列，有两种做法。一种是逻辑上的转换，即将在 where 中涉及到两个表关联或者 filter 的条件提前至相应的 join 操作中，从而减少了参与 join 的数据量以及最终 join 结果的数据量。还有一种做法是基于成本的做法，通过启用成本优化器，以及对 join 的表进行统计，spark 会根据 join 的成本选择代价最小的 join 方式。



题目二:架构设计题

你是某互联网公司的大数据平台架构师，请设计一套基于 Lambda 架构的数据平台架构，要求尽可能多的把课程中涉及的组件添加到该架构图中。并描述 Lambda 架构的优缺点，要求不少于 300 字。



答：



架构图如下。


Lamda 架构的优点如下，



鲁棒性和容错能力。由于批处理层被设计为追加式，即包含了自开始以来的整体数据集，因此该系统具有一定的容错能力。如果任何数据被损坏，该架构则可以删除从损坏点以来的所有数据，并替换为正确的数据。同时，批处理视图也可以被换成完全被重新计算出的视图。而且速度层可以被丢弃。此外，在生成一组新的批处理视图的同时，该架构可以重置整个系统，使之重新运行。

可扩展性。Lambda 体系架构的实现大多是分布式系统构建的，而分布式系统基本上都是支持水平扩展的，因此，Lambda 架构是具有可扩展性的。

通用性。由于 Lambda 体系架构是一般范式，因此用户并不会被锁定在计算批处理视图的某个特定方式中。而且批处理视图和速度层的计算，可以被设计为满足某个数据系统的特定需求。

延展性。随着新的数据类型被导入，数据系统也会产生新的视图。数据系统不会被锁定在某类、或一定数量的批处理视图中。新的视图会在完成编码之后，被添加到系统中，其对应的资源也会得到轻松地延展。

可调试性。Lambda 体系架构通过每一层的输入和输出，极大地简化了计算和查询的调试。

低延迟的读取和更新。在 Lambda 体系架构中，速度层为大数据系统提供了对于最新数据集的实时查询。



Lamda 架构的缺点如下，

因为要对数据进行大量存储，并且根据业务需求，两系统可能同时需要占用资源，对资源需求大。

部署复杂，需要部署离线及实时计算两套系统，给运维造成的负担比较重。

两种计算口径，业务需要根据不同口径分开编码维护，数据源的任何变化均涉及到两个部署的修改，任务量大，难以灵活应对。

随着数据量的急剧增加、批处理窗口时间内可能无法完成处理，并对存储也挑战巨大。



题目三:简答题(三选一)

A:简述 HDFS 的读写流程，要求不少于 300 字

B:简述 Spark Shuffle 的工作原理，要求不少于 300 字

C:简述 Flink SQL 的工作原理，要求不少于 300 字



答:

Spark Shuffle 的工作原理如下，



shuffle 在 spark 中是一个内部的操作，当执行一个需要把当前的 RDD 分区给拆分到多个子分区的算子操作时(例如: repartition, groupByKey, join 等)，spark 便会触发 shuffle 过程。spark shuffle 的实现方式有 hash shuffle，sort shuffle，从 spark 2.0 开始，spark 中的 shuffle 只有 sort shuffle 这一种方式了。



shuffle 分为 shuffle write 和 shuffle read2 个阶段。

第一阶段 shuffle write 会将 RDD 按照新的分区规则进行重分区，然后将重新分区后的数据写到 executor 上的临时文件。

第二阶段 shuffle read 则会对上一阶段生成的临时文件进行读取，形成新的 RDD。