## 部署到服务器

上传编译后的运行文件hbasetest-1.0-SNAPSHOT.jar到服务器的home目录

## 启用HBase环境变量

```
source env.sh

# 内容
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/lib/hbase-current/lib/*
```

## 运行Hbase程序

```
# 运行jar程序
hadoop jar ./hbasetest-1.0-SNAPSHOT.jar
```

## 程序运行过程

第一步：连接hbase服务器emr-worker-2.cluster-285604:2181

第二步：建表zhanghui:student，包含3个列蔟name、info、score

第三步：插入题中给出的4组数据，RowKey从1-4

第四步：插入name为张煇、info:student_id为G20210607040077的1组数据，Rowkey为5

第五步：查找Roweky为5的数据

第六步：删除Rowkey为1的数据

第七步：删除整个表

## 运行结果

![image-20220326231041678](https://picbit-1255879985.file.myqcloud.com/blog/2022-03-26-151042.png)

## 代码说明

### 配置文件pom.xml

```xml
<dependencies>
    <dependency>
      <groupId>org.apache.hbase</groupId>
      <artifactId>hbase-client</artifactId>
      <version>2.3.4</version>
    </dependency>
    <dependency>
      <groupId>org.apache.hbase</groupId>
      <artifactId>hbase-it</artifactId>
      <version>2.3.4</version>
    </dependency>

  </dependencies>
```

### 主程序BaseTest.java

```java
// 建立连接
Configuration configuration = HBaseConfiguration.create();
configuration.set("hbase.zookeeper.quorum", "emr-worker-2.cluster-285604");
configuration.set("hbase.zookeeper.property.clientPort", "2181");
configuration.set("hbase.master", "emr-worker-2.cluster-285604:60000");
Connection conn = ConnectionFactory.createConnection(configuration);
Admin admin = conn.getAdmin();
TableName tableName = TableName.valueOf("zhanghui:student");

// 建表
if (admin.tableExists(tableName)) {
  System.out.println("Table already exists");
} else {
  HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
  HColumnDescriptor hColumnDescriptor1 = new HColumnDescriptor("name");
  hTableDescriptor.addFamily(hColumnDescriptor1);
  HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor("info");
  hTableDescriptor.addFamily(hColumnDescriptor2);
  HColumnDescriptor hColumnDescriptor3 = new HColumnDescriptor("score");
  hTableDescriptor.addFamily(hColumnDescriptor3);
  admin.createTable(hTableDescriptor);
  System.out.println("Table create successful");
}

// 插入数据
Put put1 = new Put(Bytes.toBytes("1")); // row key
put1.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes("Tom"));
put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000001"));
put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("1"));
put1.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("75"));
put1.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("82"));
conn.getTable(tableName).put(put1);
System.out.println("Data insert success");

Put put2 = new Put(Bytes.toBytes("2")); // row key
put2.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes("Tom"));
put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000002"));
put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("1"));
put2.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("85"));
put2.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("67"));
conn.getTable(tableName).put(put2);
System.out.println("Data insert success");

Put put3 = new Put(Bytes.toBytes("3")); // row key
put3.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes("Jack"));
put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000003"));
put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
put3.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("80"));
put3.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("80"));
conn.getTable(tableName).put(put3);
System.out.println("Data insert success");

Put put4 = new Put(Bytes.toBytes("4")); // row key
put4.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes("Rose"));
put4.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000004"));
put4.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
put4.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("60"));
put4.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("61"));
conn.getTable(tableName).put(put4);
System.out.println("Data insert success");

Put put5 = new Put(Bytes.toBytes("5")); // row key
put5.addColumn(Bytes.toBytes("name"), null, Bytes.toBytes("张煇"));
put5.addColumn(Bytes.toBytes("info"), Bytes.toBytes("student_id"), Bytes.toBytes("G20210607040077"));
put5.addColumn(Bytes.toBytes("info"), Bytes.toBytes("class"), Bytes.toBytes("2"));
put5.addColumn(Bytes.toBytes("score"), Bytes.toBytes("understanding"), Bytes.toBytes("85"));
put5.addColumn(Bytes.toBytes("score"), Bytes.toBytes("programming"), Bytes.toBytes("85"));
conn.getTable(tableName).put(put5);
System.out.println("Data insert success");

// 查看数据
Get get = new Get(Bytes.toBytes("5")); // 指定rowKey
if (!get.isCheckExistenceOnly()) {
  Result result = conn.getTable(tableName).get(get);
  for (Cell cell : result.rawCells()) {
    String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),cell.getQualifierLength());
    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
    System.out.println("Data get success, colName: " + colName + ", value: " + value);
  }
}

// 删除数据
Delete delete = new Delete(Bytes.toBytes("1")); // 指定rowKey
conn.getTable(tableName).delete(delete);
System.out.println("Delete Success");

// 删除表
if (admin.tableExists(tableName)) {
  admin.disableTable(tableName);
  admin.deleteTable(tableName);
  System.out.println("Table Delete Successful");
} else {
  System.out.println("Table does not exist!");
}
```

