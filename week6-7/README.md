# 作业一：使用 RDD API 实现带词频的倒排索引

倒排索引（Inverted index），也被称为反向索引。它是文档检索系统中最常用的数据结构。被广泛地应用于全文搜索引擎。

## 代码说明

### 配置文件pom.xml
```xml
<groupId>com.zhanghui</groupId>
<artifactId>SparkTest</artifactId>
<version>1.0-SNAPSHOT</version>

<properties>
  <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
  <maven.compiler.source>8</maven.compiler.source>
  <maven.compiler.target>8</maven.compiler.target>
  <spark.version>3.1.2</spark.version>
</properties>

<dependencies>
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-avro_2.12</artifactId>
    <version>${spark.version}</version>
  </dependency>
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.12</artifactId>
    <version>${spark.version}</version>
  </dependency>
</dependencies>
```
### 主程序InvertIndex.java
```java
public class InvertIndex {
    
    public static void main(String [] args) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.master", "local[*]");
        sparkConf.set("spark.app.name", "localrun");
        SparkContext sparkContext = SparkContext.getOrCreate(sparkConf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
        
        // 数据文件目录为第一参数
        JavaPairRDD<String, String> fileNameContentsRDD = javaSparkContext.wholeTextFiles(args[0], 1);
        JavaPairRDD<String, String> wordFileNameRDD = fileNameContentsRDD.flatMapToPair((PairFlatMapFunction<Tuple2<String, String>, String, String>) fileNameContentPair -> {
            String fileName = getFileName(fileNameContentPair._1());
            String content = fileNameContentPair._2();
            String [] lines = content.split("[\r\n]");
            List<Tuple2<String, String>> fileNameWordPairs = new ArrayList<>(lines.length);
            for(String line : lines){
                String [] wordsInCurrentLine = line.split(" ");
                fileNameWordPairs.addAll(Arrays.stream(wordsInCurrentLine).map(word -> new Tuple2<>(word, fileName)).collect(Collectors.toList()));
            }
            return fileNameWordPairs.iterator();
        });
        
        JavaPairRDD<Tuple2<String, String>, Integer> wordFileNameCountPerPairs = wordFileNameRDD.mapToPair(wordFileNamePair -> new Tuple2<>(wordFileNamePair, 1))
            .reduceByKey(Integer::sum);
        JavaPairRDD<String, Tuple2<String, Integer>> wordCountPerFileNamePairs = wordFileNameCountPerPairs.mapToPair(wordFileNameCountPerPair -> new Tuple2<>(wordFileNameCountPerPair._1._1, new Tuple2<>(wordFileNameCountPerPair._1._2, wordFileNameCountPerPair._2)));
        JavaPairRDD<String, String> result = wordCountPerFileNamePairs.groupByKey().mapToPair(wordCountPerFileNamePairIterator -> new Tuple2<>(wordCountPerFileNamePairIterator._1, StringUtils.join(wordCountPerFileNamePairIterator._2.iterator(), ','))).sortByKey();
        for(Tuple2<String, String> pair : result.collect()) {
            System.out.printf("\"%s\", {%s}%n", pair._1, pair._2);
        }
    }
    
    private static String getFileName(String s) {
        return s.substring(s.lastIndexOf('/') + 1);
    }
    
}
```
### 数据文件
```bash
$ mkdir input

$ cat 0
it is what it is

$ cat 1
what is it

$ cat 2
it is a banana
```
### IDE运行测试定义
main方法第一参数为数据文件所在目录路径
![1](https://tva1.sinaimg.cn/large/e6c9d24ely1h14ua7reavj20z40m6mz0.jpg)

### IDE运行测试
![2](https://tva1.sinaimg.cn/large/e6c9d24ely1h14uadpnvjj20z40cotbo.jpg)

## spark-submit运行测试
```bash
$ spark-submit --class com.zhanghui.InvertIndex ./SparkTest-1.0-SNAPSHOT.jar ../input

22/04/09 09:50:50 INFO DAGScheduler: Job 0 finished: collect at InvertIndex.java:42, took 0.921715 s
"a", {(2,1)}
"banana", {(2,1)}
"is", {(2,1),(1,1),(0,2)}
"it", {(1,1),(0,2),(2,1)}
"what", {(0,1),(1,1)}
```

## spark-shell运行测试
```bash
$ spark-shell --jars SparkTest-1.0-SNAPSHOT.jar

scala> com.zhanghui.InvertIndex.main(Array("/Users/zhanghui/code/geektime-bigdata/week6-7/SparkTest/input"))

22/04/09 09:49:18 WARN SparkContext: Using an existing SparkContext; some configuration may not take effect.
"a", {(2,1)}                                                                    
"banana", {(2,1)}
"is", {(2,1),(1,1),(0,2)}
"it", {(1,1),(0,2),(2,1)}
"what", {(0,1),(1,1)}
```

# 作业二：Distcp 的 Spark 实现

使用Spark实现Hadoop分布式数据传输工具DistCp (distributed copy)，只要求实现最基础的copy功能，对于-update、-diff、-p不做要求。

## 代码说明

### 配置文件pom.xml

同上一个作业

### 主程序SparkDistCp.java

main 方法的第一个参数为源端目录路径，第二个参数为目标端目录路径，第三个参数为最大并发任务数，第四个参数为是否忽略失败
```java
package com.zhanghui;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
* @author Zhanghui
* @date 2022.04.09
*/
public class SparkDistCp {
    private static final SparkConf sparkConf;
    private static final SparkContext sparkContext;
    private static final JavaSparkContext javaSparkContext;
    private static final Configuration configuration;
    
    static {
        sparkConf = new SparkConf();
        sparkConf.set("spark.master", "local[*]");
        sparkConf.set("spark.app.name", "localrun");
        
        sparkContext = SparkContext.getOrCreate(sparkConf);
        javaSparkContext = new JavaSparkContext(sparkContext);
        configuration = sparkContext.hadoopConfiguration();
    }
    
    public static void main(String [] args) throws IOException {
        // 源目录，如本地file:///tmp/dir1/
        String sourceRootPathStr = args[0];
        // 目的目录，如本地file:///tmp/dir2/
        String targetRootPathStr = args[1];
        // 最大并行任务数
        int maxConcurrency = Integer.parseInt(args[2]);
        // 是否忽略失败
        boolean ignoreFailure = Boolean.parseBoolean(args[3]);
        
        JavaRDD<String> sourceFileListRDD = getSourceFileLists(sourceRootPathStr, targetRootPathStr, maxConcurrency);
        sourceFileListRDD.foreachPartition(sourceFileIterator -> {
            FileSystem sourceFileSystem = new Path(sourceRootPathStr).getFileSystem(configuration);
            FileSystem targetFileSystem = new Path(targetRootPathStr).getFileSystem(configuration);
            while(sourceFileIterator.hasNext()) {
                String sourceFilePath = sourceFileIterator.next();
                Path sourceFileRelativePath = new Path(new Path(sourceRootPathStr).toUri().relativize(new Path(sourceFilePath).toUri()));
                Path targetPath = new Path(targetRootPathStr, sourceFileRelativePath);
                try(InputStream sourceInputStream = sourceFileSystem.open(new Path(sourceFilePath));
                    FSDataOutputStream targetOutputStream = targetFileSystem.create(targetPath, true)) {
                    IOUtils.copy(sourceInputStream, targetOutputStream);
                } catch(Throwable t) {
                    if(!ignoreFailure) {
                        throw t;
                    }
                }
            }
        });
    }
    
    private static JavaRDD<String> getSourceFileLists(String sourceRootPathStr, String targetRootPathStr, int maxConcurrency) throws IOException {
        Path sourceRootPath = new Path(sourceRootPathStr);
        Path targetRootPath = new Path(targetRootPathStr);
        FileSystem sourceFileSystem = sourceRootPath.getFileSystem(configuration);
        FileSystem targetFileSystem = targetRootPath.getFileSystem(configuration);
        RemoteIterator<LocatedFileStatus> iterator = sourceFileSystem.listFiles(sourceRootPath, true);
        Set<Path> distinctDirPaths = new HashSet<>();
        List<String> fileList = new ArrayList<>();
        while(iterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = iterator.next();
            Path filePath = locatedFileStatus.getPath();
            distinctDirPaths.add(filePath.getParent());
            fileList.add(filePath.toString());
        }
        distinctDirPaths.remove(sourceRootPath);
        for(Path distinctDirPath : distinctDirPaths) {
            String sourceChildrenDirRelativePathStr = sourceRootPath.toUri().relativize(distinctDirPath.toUri()).toString();
            targetFileSystem.mkdirs(new Path(targetRootPath, sourceChildrenDirRelativePathStr), new FsPermission(FsAction.ALL, FsAction.READ, FsAction.READ));
        }
        return javaSparkContext.parallelize(fileList, maxConcurrency);
    }
}

```
## spark-submit本地文件测试
```bash
$ spark-submit --class com.zhanghui.SparkDistCp ./SparkTest-1.0-SNAPSHOT.jar file:///tmp/dir1 file:///tmp/dir2 3 false

2022-04-10 17:58:47,357 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 607 ms on 192.168.1.10 (executor driver) (1/3)
2022-04-10 17:58:47,359 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 567 ms on 192.168.1.10 (executor driver) (2/3)
2022-04-10 17:58:47,359 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 568 ms on 192.168.1.10 (executor driver) (3/3)
2022-04-10 17:58:47,380 INFO scheduler.DAGScheduler: Job 0 finished: foreachPartition at SparkDistCp.java:45, took 0.887878 s

# 运行之前
$ tree /tmp/dir1
/tmp/dir1
├── 1
│   ├── 1-2
│   │   └── me.txt
│   ├── 1.txt
│   └── 2.txt
└── uhub.pdf

$ tree /tmp/dir2
/tmp/dir2

# 运行之后
$ tree /tmp/dir2
/tmp/dir2
├── 1
│   ├── 1-2
│   │   └── me.txt
│   ├── 1.txt
│   └── 2.txt
└── uhub.pdf
```

## spark-submit hdfs文件测试

```bash
$ spark-submit --class com.zhanghui.SparkDistCp ./SparkTest-1.0-SNAPSHOT.jar hdfs://localhost:9000/tmp/dir1 hdfs://localhost:9000/tmp/dir2 3 false

2022-04-10 19:20:52,505 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 680 ms on 192.168.1.10 (executor driver) (1/3)
2022-04-10 19:20:52,507 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 560 ms on 192.168.1.10 (executor driver) (2/3)
2022-04-10 19:20:52,766 INFO executor.Executor: Finished task 2.0 in stage 0.0 (TID 2). 880 bytes result sent to driver
2022-04-10 19:20:52,767 INFO scheduler.TaskSetManager: Finished task 2.0 in stage 0.0 (TID 2) in 819 ms on 192.168.1.10 (executor driver) (3/3)
2022-04-10 19:20:52,778 INFO scheduler.DAGScheduler: Job 0 finished: foreachPartition at SparkDistCp.java:48, took 1.248412 s

# 运行之后
$ hadoop fs -ls /tmp/dir2  
2022-04-10 19:21:02,296 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Found 3 items
drwxr--r--   - zhanghui supergroup          0 2022-04-10 19:20 /tmp/dir2/1
-rw-r--r--   1 zhanghui supergroup          0 2022-04-10 19:20 /tmp/dir2/1.txt
-rw-r--r--   1 zhangh
```



## spark-shell运行测试

```bash
$ spark-shell --jars SparkTest-1.0-SNAPSHOT.jar

scala> com.zhanghui.SparkDistCp.main(Array("file:///tmp/dir1","file:///tmp/dir2","3","false"))
2022-04-10 17:42:21,251 WARN spark.SparkContext: Using an existing SparkContext; some configuration may not take effect.

# 运行结果
同上本地
```

