# 作业一：实现 Compact table command

## 作业要求

要求：

添加 compact table 命令，用于合并小文件，例如表 test1 总共有 50000 个文件，每个 1MB，通过该命令，合成为 500 个文件，每个约 100MB。

语法：

COMPACT TABLE table_identify [partitionSpec] [INTO fileNum FILES]；

说明：

基本要求是完成以下功能：COMPACT TABLE test1 INTO 500 FILES；

如果添加 partitionSpec，则只合并指定的 partition 目录的文件；

如果不加 into fileNum files，则把表中的文件合并成 128MB 大小。

## 代码说明

### 修改SqlBase.g4

添加语法规则，路径sql/catalyst/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4

```xml
# statement下添加
| COMPACT TABLE target=tableIdentifier partitionSpec?
(INTO fileNum=INTEGER_VALUE FILES)?                        #compactTable

//--ANSI-NON-RESERVED-START下添加
| FILES

#nonReserved下添加
| FILES

//--SPARK-KEYWORD-LIST-START下添加
FILES:'FILES';
```
### 编译DSL

生成CompactTableContext类，路径sql/catalyst/target/generated-sources/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.java

```bash
build/mvn org.antlr:antlr4-maven-plugin:4.8:antlr4
```

### 修改SparkSqlParser.scala

添加一个visitCompactTable()方法，在visitCompactTable()方法中去调用CompactTableCommand()样例类，路径sql/core/src/main/scala/org/apache/spark/sql/execution/SparkSqlParser.scala

```scala
  override def visitCompactTable(
      ctx: CompactTableContext): LogicalPlan = withOrigin(ctx) {
    val table = visitTableIdentifier(ctx.tableIdentifier())
    val filesNum = if (ctx.INTEGER_VALUE() != null) {
      Some(ctx.INTEGER_VALUE().getText)
    } else {
      None
    }
    val partition = if (ctx.partitionSpec() != null) {
      Some(ctx.partitionSpec().getText)
    } else {
      None
    }
    CompactTableCommand(table, filesNum, partition);
  }
```
### 添加CompactTableCommand.scala

创建CompactTableCommand()样例类，定义调用方法，路径sql/core/src/main/scala/org/apache/spark/sql/execution/command/CompactTableCommand.scala

```scala
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class CompactTableCommand(table: TableIdentifier,
                               filesNum: Option[String],
                               partitionSpec: Option[String]) extends LeafRunnableCommand {

  private val defaultSize = 128 * 1024 * 1024

  override def output: Seq[Attribute] = Seq(
    AttributeReference("COMPACT_TABLE", StringType, nullable = false)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.catalog.setCurrentDatabase(table.database.getOrElse("default"))


    val tempTableName = "`" + table.identifier + "_" + System.currentTimeMillis() + "`"

    val originDataFrame = sparkSession.table(table.identifier)
    val partitions = filesNum match {
      case Some(files) => files.toInt
      case None => (sparkSession.sessionState
        .executePlan(originDataFrame.queryExecution.logical)
        .optimizedPlan.stats.sizeInBytes / defaultSize).toInt + 1
    }
    // scalastyle:off println
    println(partitions, tempTableName)

    if (partitionSpec.nonEmpty) {
      // overwrite-specific-partitions-in-spark-dataframe-write-method
      sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

      val conditionExpr = partitionSpec.get.trim.stripPrefix("partition(").dropRight(1)
        .replace(",", "AND")
      // scalastyle:off println
      println(conditionExpr)

      originDataFrame.where(conditionExpr).repartition(partitions)
        .write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tempTableName)

      sparkSession.table(tempTableName).write
        .mode(SaveMode.Overwrite)
        .insertInto(table.identifier)
    } else {
      originDataFrame.repartition(partitions)
        .write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tempTableName)

      sparkSession.table(tempTableName)
        .write
        .mode(SaveMode.Overwrite)
        .saveAsTable(table.identifier)
    }

    Seq(Row(s"compact table ${table.identifier} finished."))
  }
}
```
### 编译

```
build/mvn package -DskipTests -Phive -Phive-thriftserver
```

### 建表并插入数据

```bash
# 启动spark-sql
/bin/spark-sql

# 创建表
create table if not exists exam
(
    id int COMMENT 'question id'
    ,ans string COMMENT 'answer choice'
);

# 插入数据
insert into exam values(1, 'a'), (2, 'b'),(3, 'd'), (4, 'a'),(5, 'c'), (6, 'd');

# 检索记录
select * from exam;
```

结果：检查spark-warehouse/exam目录，可以看到生成了6个文件

![6](https://tva1.sinaimg.cn/large/e6c9d24ely1h2obcuxg6xj214m09mdiq.jpg)

### 压缩为3个文件

```bash
compact table exam into 3 files;
```

结果：检查spark-warehouse/exam目录，可以看到压缩为3个文件

![3](https://tva1.sinaimg.cn/large/e6c9d24ely1h2obd1uinrj20x407q75y.jpg)

### 默认压缩

按照spark.files.maxPartitionBytes指定的值128MB进行分区

```bash
compact table exam;
```

结果：检查spark-warehouse/exam目录，可以看到压缩为1个文件

![1](https://tva1.sinaimg.cn/large/e6c9d24ely1h2obd71vbwj213w04yaax.jpg)
