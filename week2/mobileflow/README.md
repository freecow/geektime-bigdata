### 部署到服务器

上传数据文件HTTP_20130313143750.dat、运行文件mobileflow-1.0-SNAPSHOT.jar到服务器的home目录

### 上传数据文件

在hdfs分区上创建目录并上传数据文件

```
# 创建输入目录input
# 输出目录output由程序运行创建
hadoop fs -mkdir /user/zhanghui/week2/input

# 上传源数据
hadoop fs -put HTTP_20130313143750.dat /user/zhanghui/week2/input
```

### 运行MapReduce

```
# 运行jar程序
# 参数1为输入input目录里的数据文件，参数2为输入到output目录，参数3表示只调用1个Reduce
hadoop jar ./mobileflow-1.0-SNAPSHOT.jar /user/zhanghui/week2/input/HTTP_20130313143750.dat /user/zhanghui/week2/output 1

# 运行完毕后显示结果
hadoop fs -ls /user/xxx/week2/output
hadoop fs -cat /user/xxx/week2/output/part-r-00000

# 删除目录
hadoop fs -rmr /user/xxx/week2/output
```

