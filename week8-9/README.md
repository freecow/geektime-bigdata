# 作业一：为Spark SQL添加一条自定义命令

## 作业要求

- SHOW VERSION；
- 显示当前Spark版本和Java版本

## 代码说明

### 修改SqlBase.g4

路径sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4

添加语法规则，总共需要添加4处

```xml
statement
    | SHOW VERSION                                                     #showVersion
ansiNonReserved
    | VERSION
nonReserved
    | VERSION
//--SPARK-KEYWORD-LIST-START
VERSION: 'VERSION';
```
### 修改SparkSqlParser.scala

路径/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkSqlParser.scala

添加一个visitShowVersion()方法，在visitShowVersion()方法中去调用ShowVersionCommand()样例类

```java
  override def visitShowVersion(ctx: ShowVersionContext): LogicalPlan = withOrigin(ctx) {
    ShowVersionCommand()
  }
```
### 添加ShowVersionCommand.scala

路径sql/core/src/main/scala/org/apache/spark/sql/execution/command/ShowVersionCommand.scala

创建ShowVersionCommand()样例类，定义调用方法，输出Spark和Java版本

```bash
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType

case class ShowVersionCommand() extends RunnableCommand{

  override val output: Seq[Attribute] =
    Seq(AttributeReference("version", StringType, nullable = true)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sparkVersion = sparkSession.version
    val javaVersion = System.getProperty("java.version")
    val outputString = "Spark Version: %s, Java Version: %s"
      .format(sparkVersion, javaVersion)

    Seq(Row(outputString))
  }

}
```
### 编译

```
build/sbt package -Phive -Phive-thriftserver
```

### 运行测试

```
./bin/spark-sql

> show version;
> quit;
```

结果示意

![image-20220422070125466](https://tva1.sinaimg.cn/large/e6c9d24ely1h1i4iodn71j214i0l0jxg.jpg)

# 作业二：构建SQL满足如下要求

## 作业要求

通过 set spark.sql.planChangeLog.level=WARN，查看：

1. 构建一条 SQL，同时 apply 下面三条优化规则：

- CombineFilters
- CollapseProject
- BooleanSimplification

2. 构建一条 SQL，同时 apply 下面五条优化规则：

- ConstantFolding
- PushDownPredicates
- ReplaceDistinctWithAggregate
- ReplaceExceptWithAntiJoin
- FoldablePropagation



## 创建示例数据表

### 创建外部json数据文件

```
# students.json
{"ID":1,"name":"LiMing","address":"Beijing","age":14,"sex":"Male"},
{"ID":2,"name":"ZhangJinChen","address":"Tianijn","age":18,"sex":"Male"},
{"ID":3,"name":"ChenBo","address":"Shanxi","age":16,"sex":"Female"},
{"ID":4,"name":"XueChongFei","address":"HeBei","age":17,"sex":"Male"},
{"ID":5,"name":"ZhuXiaoJuan","address":"XinJiang","age":19,"sex":"FeMale"}
{"ID":6,"name":"YangYong","address":"XinJiang","age":18,"sex":"Male"}
{"ID":7,"name":"LiLong","address":"Shanxi","age":19,"sex":"FeMale"}
```

### 创建表

```sql
# 进入spark-sql
./bin/spark-sql

# 创建表
DROP TABLE students;
CREATE TEMPORARY TABLE students USING org.apache.spark.sql.json OPTIONS (path 'students.json');

# 查看记录
select * from students;
1       Beijing 14      LiMing  Male
2       Tianijn 18      ZhangJinChen    Male
3       Shanxi  16      ChenBo  Female
4       HeBei   17      XueChongFei     Male
5       XinJiang        19      ZhuXiaoJuan     FeMale
6       XinJiang        18      YangYong        Male
7       Shanxi  19      LiLong  FeMale
Time taken: 0.196 seconds, Fetched 7 row(s)

# 设置日志级别
set spark.sql.planChangeLog.level=WARN;
...
spark.sql.planChangeLog.level   WARN
Time taken: 0.057 seconds, Fetched 1 row(s)
```

## 构建第一条SQL

### 运行sql

```SQL
select s.address from (select name,address,age,sex from students where 1="1" and age > 17) s where s.age<20 and s.sex="FeMale";
```
### 优化结果

```bash
22/05/02 18:55:50 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 Project [address#8]                                                        Project [address#8]
!+- Filter ((age#9L < cast(20 as bigint)) AND (sex#11 = FeMale))            +- Project [name#10, address#8, age#9L, sex#11]
!   +- Project [name#10, address#8, age#9L, sex#11]                            +- Filter (((1 = cast(1 as int)) AND (age#9L > cast(17 as bigint))) AND ((age#9L < cast(20 as bigint)) AND (sex#11 = FeMale)))
!      +- Filter ((1 = cast(1 as int)) AND (age#9L > cast(17 as bigint)))         +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json
!         +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json           
           
22/05/02 18:55:50 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ColumnPruning ===
 Project [address#8]                                                                                                                 Project [address#8]
!+- Project [name#10, address#8, age#9L, sex#11]                                                                                     +- Project [address#8]
    +- Filter (((1 = cast(1 as int)) AND (age#9L > cast(17 as bigint))) AND ((age#9L < cast(20 as bigint)) AND (sex#11 = FeMale)))      +- Filter (((1 = cast(1 as int)) AND (age#9L > cast(17 as bigint))) AND ((age#9L < cast(20 as bigint)) AND (sex#11 = FeMale)))
       +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json                                                                             +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json
           
22/05/02 18:55:50 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.CollapseProject ===
 Project [address#8]                                                                                                                 Project [address#8]
!+- Project [address#8]                                                                                                              +- Filter (((1 = cast(1 as int)) AND (age#9L > cast(17 as bigint))) AND ((age#9L < cast(20 as bigint)) AND (sex#11 = FeMale)))
!   +- Filter (((1 = cast(1 as int)) AND (age#9L > cast(17 as bigint))) AND ((age#9L < cast(20 as bigint)) AND (sex#11 = FeMale)))      +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json
!      +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json                                                                       
           
22/05/02 18:55:50 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 Project [address#8]                                                                                                              Project [address#8]
!+- Filter (((1 = cast(1 as int)) AND (age#9L > cast(17 as bigint))) AND ((age#9L < cast(20 as bigint)) AND (sex#11 = FeMale)))   +- Filter ((true AND (age#9L > 17)) AND ((age#9L < 20) AND (sex#11 = FeMale)))
    +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json                                                                          +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json
           
22/05/02 18:55:50 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.BooleanSimplification ===
 Project [address#8]                                                              Project [address#8]
!+- Filter ((true AND (age#9L > 17)) AND ((age#9L < 20) AND (sex#11 = FeMale)))   +- Filter ((age#9L > 17) AND ((age#9L < 20) AND (sex#11 = FeMale)))
    +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json                          +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json


...
XinJiang
Shanxi
Time taken: 0.192 seconds, Fetched 2 row(s)
```

## 构建第二条SQL

### 运行SQL

```bash
(select a.address, a.age + (100 + 80), Now() z from (select distinct name, age, address from students) a where a.age>15 order by z) except (select a.address, a.age + (100 + 80), Now() z from (select distinct name, age, address from students) a where a.name="YangYong");
```

### 优化结果

```bash
22/05/02 19:08:51 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 Aggregate [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000], [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000 AS z#68]                                 Aggregate [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000], [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000 AS z#68]
 +- Sort [1651489731959000 ASC NULLS FIRST], true                                                                                                                                               +- Sort [1651489731959000 ASC NULLS FIRST], true
!   +- Aggregate [name#10, age#9L, address#8], [address#8, (age#9L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000 AS z#68]                              +- Aggregate [name#10, age#9L, address#8], [address#8, (age#9L + 180) AS (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000 AS z#68]
       +- Project [name#10, age#9L, address#8]                                                                                                                                                        +- Project [name#10, age#9L, address#8]
!         +- Join LeftAnti, (((address#8 <=> address#73) AND ((age#9L + cast((100 + 80) as bigint)) <=> (age + CAST((100 + 80) AS BIGINT))#70L)) AND (1651489731959000 <=> 1651489731959000))            +- Join LeftAnti, (((address#8 <=> address#73) AND ((age#9L + 180) <=> (age + CAST((100 + 80) AS BIGINT))#70L)) AND true)
             :- Project [address#8, age#9L, name#10]                                                                                                                                                        :- Project [address#8, age#9L, name#10]
!            :  +- Filter (age#9L > cast(15 as bigint))                                                                                                                                                     :  +- Filter (age#9L > 15)
             :     +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json                                                                                                                                  :     +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json
!            +- Aggregate [name#75, age#74L, address#73], [address#73, (age#74L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#70L, 1651489731959000 AS z#69]                          +- Aggregate [name#75, age#74L, address#73], [address#73, (age#74L + 180) AS (age + CAST((100 + 80) AS BIGINT))#70L, 1651489731959000 AS z#69]
                +- Project [name#75, age#74L, address#73]                                                                                                                                                      +- Project [name#75, age#74L, address#73]
                   +- Filter (name#75 = YangYong)                                                                                                                                                                 +- Filter (name#75 = YangYong)
                      +- Relation[ID#72L,address#73,age#74L,name#75,sex#76] json                                                                                                                                     +- Relation[ID#72L,address#73,age#74L,name#75,sex#76] json

22/05/02 19:08:51 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.PushDownPredicates ===
 Aggregate [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, z#68], [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, z#68]                                 Aggregate [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, z#68], [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, z#68]
 +- Join LeftAnti, (((address#8 <=> address#73) AND ((age + CAST((100 + 80) AS BIGINT))#71L <=> (age + CAST((100 + 80) AS BIGINT))#70L)) AND (z#68 <=> z#69))   +- Join LeftAnti, (((address#8 <=> address#73) AND ((age + CAST((100 + 80) AS BIGINT))#71L <=> (age + CAST((100 + 80) AS BIGINT))#70L)) AND (z#68 <=> z#69))
    :- Sort [z#68 ASC NULLS FIRST], true                                                                                                                           :- Sort [z#68 ASC NULLS FIRST], true
    :  +- Project [address#8, (age#9L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000 AS z#68]                           :  +- Project [address#8, (age#9L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000 AS z#68]
!   :     +- Filter (age#9L > cast(15 as bigint))                                                                                                                  :     +- Aggregate [name#10, age#9L, address#8], [name#10, age#9L, address#8]
!   :        +- Aggregate [name#10, age#9L, address#8], [name#10, age#9L, address#8]                                                                               :        +- Project [name#10, age#9L, address#8]
!   :           +- Project [name#10, age#9L, address#8]                                                                                                            :           +- Filter (age#9L > cast(15 as bigint))
    :              +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json                                                                                         :              +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json
    +- Project [address#73, (age#74L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#70L, 1651489731959000 AS z#69]                            +- Project [address#73, (age#74L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#70L, 1651489731959000 AS z#69]
!      +- Filter (name#75 = YangYong)                                                                                                                                 +- Aggregate [name#75, age#74L, address#73], [name#75, age#74L, address#73]
!         +- Aggregate [name#75, age#74L, address#73], [name#75, age#74L, address#73]                                                                                    +- Project [name#75, age#74L, address#73]
!            +- Project [name#75, age#74L, address#73]                                                                                                                      +- Filter (name#75 = YangYong)
                +- Relation[ID#72L,address#73,age#74L,name#75,sex#76] json                                                                                                     +- Relation[ID#72L,address#73,age#74L,name#75,sex#76] json
           
22/05/02 19:08:51 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ReplaceDistinctWithAggregate ===
!Distinct                                                                                                                                                       Aggregate [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, z#68], [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, z#68]
 +- Join LeftAnti, (((address#8 <=> address#73) AND ((age + CAST((100 + 80) AS BIGINT))#71L <=> (age + CAST((100 + 80) AS BIGINT))#70L)) AND (z#68 <=> z#69))   +- Join LeftAnti, (((address#8 <=> address#73) AND ((age + CAST((100 + 80) AS BIGINT))#71L <=> (age + CAST((100 + 80) AS BIGINT))#70L)) AND (z#68 <=> z#69))
    :- Sort [z#68 ASC NULLS FIRST], true                                                                                                                           :- Sort [z#68 ASC NULLS FIRST], true
    :  +- Project [address#8, (age#9L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000 AS z#68]                           :  +- Project [address#8, (age#9L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000 AS z#68]
    :     +- Filter (age#9L > cast(15 as bigint))                                                                                                                  :     +- Filter (age#9L > cast(15 as bigint))
!   :        +- Distinct                                                                                                                                           :        +- Aggregate [name#10, age#9L, address#8], [name#10, age#9L, address#8]
    :           +- Project [name#10, age#9L, address#8]                                                                                                            :           +- Project [name#10, age#9L, address#8]
    :              +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json                                                                                         :              +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json
    +- Project [address#73, (age#74L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#70L, 1651489731959000 AS z#69]                            +- Project [address#73, (age#74L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#70L, 1651489731959000 AS z#69]
       +- Filter (name#75 = YangYong)                                                                                                                                 +- Filter (name#75 = YangYong)
!         +- Distinct                                                                                                                                                    +- Aggregate [name#75, age#74L, address#73], [name#75, age#74L, address#73]
             +- Project [name#75, age#74L, address#73]                                                                                                                      +- Project [name#75, age#74L, address#73]
                +- Relation[ID#72L,address#73,age#74L,name#75,sex#76] json                                                                                                     +- Relation[ID#72L,address#73,age#74L,name#75,sex#76] json
                
22/05/02 19:08:51 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ReplaceExceptWithAntiJoin ===
!Except false                                                                                                                           Distinct
!:- Sort [z#68 ASC NULLS FIRST], true                                                                                                   +- Join LeftAnti, (((address#8 <=> address#73) AND ((age + CAST((100 + 80) AS BIGINT))#71L <=> (age + CAST((100 + 80) AS BIGINT))#70L)) AND (z#68 <=> z#69))
!:  +- Project [address#8, (age#9L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000 AS z#68]      :- Sort [z#68 ASC NULLS FIRST], true
!:     +- Filter (age#9L > cast(15 as bigint))                                                                                             :  +- Project [address#8, (age#9L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000 AS z#68]
!:        +- Distinct                                                                                                                      :     +- Filter (age#9L > cast(15 as bigint))
!:           +- Project [name#10, age#9L, address#8]                                                                                       :        +- Distinct
!:              +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json                                                                    :           +- Project [name#10, age#9L, address#8]
!+- Project [address#73, (age#74L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#70L, 1651489731959000 AS z#69]       :              +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json
!   +- Filter (name#75 = YangYong)                                                                                                         +- Project [address#73, (age#74L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#70L, 1651489731959000 AS z#69]
!      +- Distinct                                                                                                                            +- Filter (name#75 = YangYong)
!         +- Project [name#75, age#74L, address#73]                                                                                              +- Distinct
!            +- Relation[ID#72L,address#73,age#74L,name#75,sex#76] json                                                                             +- Project [name#75, age#74L, address#73]
!                                                                                                                                                      +- Relation[ID#72L,address#73,age#74L,name#75,sex#76] json

22/05/02 19:08:51 WARN PlanChangeLogger: 
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.FoldablePropagation ===
!Aggregate [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, z#68], [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, z#68]                                                      Aggregate [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000], [address#8, (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000 AS z#68]
!+- Sort [z#68 ASC NULLS FIRST], true                                                                                                                                                +- Sort [1651489731959000 ASC NULLS FIRST], true
    +- Aggregate [name#10, age#9L, address#8], [address#8, (age#9L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000 AS z#68]                   +- Aggregate [name#10, age#9L, address#8], [address#8, (age#9L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#71L, 1651489731959000 AS z#68]
       +- Project [name#10, age#9L, address#8]                                                                                                                                             +- Project [name#10, age#9L, address#8]
!         +- Join LeftAnti, (((address#8 <=> address#73) AND ((age#9L + cast((100 + 80) as bigint)) <=> (age + CAST((100 + 80) AS BIGINT))#70L)) AND (1651489731959000 <=> z#69))             +- Join LeftAnti, (((address#8 <=> address#73) AND ((age#9L + cast((100 + 80) as bigint)) <=> (age + CAST((100 + 80) AS BIGINT))#70L)) AND (1651489731959000 <=> 1651489731959000))
             :- Project [address#8, age#9L, name#10]                                                                                                                                             :- Project [address#8, age#9L, name#10]
             :  +- Filter (age#9L > cast(15 as bigint))                                                                                                                                          :  +- Filter (age#9L > cast(15 as bigint))
             :     +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json                                                                                                                       :     +- Relation[ID#7L,address#8,age#9L,name#10,sex#11] json
             +- Aggregate [name#75, age#74L, address#73], [address#73, (age#74L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#70L, 1651489731959000 AS z#69]               +- Aggregate [name#75, age#74L, address#73], [address#73, (age#74L + cast((100 + 80) as bigint)) AS (age + CAST((100 + 80) AS BIGINT))#70L, 1651489731959000 AS z#69]
                +- Project [name#75, age#74L, address#73]                                                                                                                                           +- Project [name#75, age#74L, address#73]
                   +- Filter (name#75 = YangYong)                                                                                                                                                      +- Filter (name#75 = YangYong)
                      +- Relation[ID#72L,address#73,age#74L,name#75,sex#76] json                                                                                                                          +- Relation[ID#72L,address#73,age#74L,name#75,sex#76] json
...
Shanxi	196	2022-05-02 19:08:51.959
XinJiang	199	2022-05-02 19:08:51.959
HeBei	197	2022-05-02 19:08:51.959
Tianijn	198	2022-05-02 19:08:51.959
Shanxi	199	2022-05-02 19:08:51.959
Time taken: 0.985 seconds, Fetched 5 row(s)
```



# 作业三：实现自定义优化规则（静默规则）

## 作业要求

- 第一步：实现自定义规则 (静默规则，通过 set spark.sql.planChangeLog.level=WARN，确认执行到就行)

  ```java
  case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] {
   def apply(plan: LogicalPlan): LogicalPlan = plan transform { .... }
  }
  ```

  

- 第二步：创建自己的 Extension 并注入

  ```java
  class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
   override def apply(extensions: SparkSessionExtensions): Unit = { 
    extensions.injectOptimizerRule { session =>
     new MyPushDown(session) 
    }
   } 
  }
  ```

  

- 第三步：通过 spark.sql.extensions 提交

  ```bash
  bin/spark-sql --jars my.jar --conf spark.sql.extensions=com.jikeshijian.MySparkSessionExtension
  ```




## 代码说明

创建项目CustomSparkExtension

```bash
.
├── pom.xml
├── src
│   ├── main
│   │   ├── java
│   │   ├── resources
│   │   └── scala
│   │       ├── MyPushDown.scala
│   │       └── MySparkSessionExtension.scala
│   └── test
│       └── java
└── target
```

MyPushDown.scala

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Sort}
import org.apache.spark.sql.catalyst.rules._
case class MyPushDown(spark: SparkSession) extends Rule[LogicalPlan] {

  private def removeTopLevelSort(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case Sort(_, _, child) => child
      case Project(fields, child) => Project(fields, removeTopLevelSort(child))
      case other => other
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Sort(_, _, child) => {
      print("Use Define MyPushDown")
      child
    }
    case other => {
      print("Use Define MyPushDown")
      logWarning(s"Optimization rule '${ruleName}' was not excluded from the optimizer")
      other
    }
  }
}
```

MySparkSessionExtension.scala

```scala
import org.apache.spark.sql.SparkSessionExtensions

class MySparkSessionExtension extends (SparkSessionExtensions => Unit)  {
  override def apply(extensions: SparkSessionExtensions): Unit =  {
    extensions.injectOptimizerRule { session =>
      new MyPushDown(session)
    }
  }
}
```

pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.zhanghui.spark.extension</groupId>
    <artifactId>CustomSparkExtension</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
        <scala.version>2.13.8</scala.version>
        <spark.version>3.1.3</spark.version>
        <encoding>UTF-8</encoding>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.12</artifactId>
            <version>${spark.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>${spark.version}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-hive_2.12</artifactId>
            <version>${spark.version}</version>
        </dependency>

        <dependency>
            <groupId>com.github.scopt</groupId>
            <artifactId>scopt_2.12</artifactId>
            <version>3.5.0</version>
        </dependency>

        <dependency>
            <groupId>org.scalatest</groupId>
            <artifactId>scalatest_2.12</artifactId>
            <version>3.2.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
    <pluginManagement>
        <plugins>
            <!-- 编译scala的插件 -->
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>3.2.2</version>
            </plugin>
            <!-- 编译java的插件 -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.5.1</version>
            </plugin>
        </plugins>
    </pluginManagement>
    <plugins>
        <plugin>
            <groupId>net.alchim31.maven</groupId>
            <artifactId>scala-maven-plugin</artifactId>
            <executions>
                <execution>
                    <id>scala-compile-first</id>
                    <phase>process-resources</phase>
                    <goals>
                        <goal>add-source</goal>
                        <goal>compile</goal>
                    </goals>
                </execution>
                <execution>
                    <id>scala-test-compile</id>
                    <phase>process-test-resources</phase>
                    <goals>
                        <goal>testCompile</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>

        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-compiler-plugin</artifactId>
            <executions>
                <execution>
                    <phase>compile</phase>
                    <goals>
                        <goal>compile</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>

    </plugins>
    </build>

</project>
```

打包生成CustomSparkExtension-1.0-SNAPSHOT.jar



## 运行测试

运行spark-sql带上jar参数

```bash
bin/spark-sql --jars CustomSparkExtension-1.0-SNAPSHOT.jar --conf spark.sql.extensions=MySparkSessionExtension
```

进入spark-sql控制台

```bash
spark-sql> set spark.sql.planChangeLog.level=WARN;

# 创建view并查询
spark-sql> drop view test1;
spark-sql> create view test1(c1) as values (1),(2),(3);
spark-sql> select * from test1;
```

查看log，可看到自定义规则信息Optimization rule 'MyPushDown' was not excluded from the optimizer

```bash
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Pullup Correlated Expressions has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Subquery has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Replace Operators has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Aggregate has no effect.
Use Define MyPushDown22/05/05 07:54:44 WARN MyPushDown: Optimization rule 'MyPushDown' was not excluded from the optimizer
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Operator Optimization before Inferring Filters has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Infer Filters has no effect.
Use Define MyPushDown22/05/05 07:54:44 WARN MyPushDown: Optimization rule 'MyPushDown' was not excluded from the optimizer
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Operator Optimization after Inferring Filters has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Push extra predicate through join has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Early Filter and Projection Push-Down has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Join Reorder has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Eliminate Sorts has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Decimal Optimizations has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Distinct Aggregate Rewrite has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Object Expressions Optimization has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch LocalRelation has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Check Cartesian Products has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch RewriteSubquery has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch NormalizeFloatingNumbers has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch ReplaceUpdateFieldsExpression has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Optimize Metadata Only Query has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch PartitionPruning has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Pushdown Filters from PartitionPruning has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Cleanup filters that cannot be pushed down has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Extract Python UDFs has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch User Provided Optimizers has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: 
=== Metrics of Executed Rules ===
Total number of runs: 154
Total time: 0.004047304 seconds
Total number of effective runs: 4
Total time of effective runs: 0.001734547 seconds
      
22/05/05 07:54:44 WARN PlanChangeLogger: Batch Preparations has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: Batch CleanExpressions has no effect.
22/05/05 07:54:44 WARN PlanChangeLogger: 
=== Metrics of Executed Rules ===
Total number of runs: 1
Total time: 5.356E-6 seconds
Total number of effective runs: 0
Total time of effective runs: 0.0 seconds
      
1
2
3
Time taken: 0.45 seconds, Fetched 3 row(s)
```

