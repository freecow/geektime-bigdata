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
```

运行过程

![image-20220326194450458](https://tva1.sinaimg.cn/large/e6c9d24ely1h0niun47zvj20nc0i3n2s.jpg)



### 显示结果

```
# 运行完毕后显示结果
hadoop fs -ls /user/zhanghui/week2/output
hadoop fs -cat /user/zhanghui/week2/output/part-r-00000

# 删除目录
hadoop fs -rmr /user/zhanghui/week2/output
```

![image-20220326194614201](https://tva1.sinaimg.cn/large/e6c9d24ely1h0niw2z6xjj20mu0chmyr.jpg)



### 代码说明

#### pom.xml定义

![image-20220326192217936](https://tva1.sinaimg.cn/large/e6c9d24ely1h0ni783gp5j20eq0h8q4z.jpg)

#### Mapper实现

![image-20220326192259385](https://tva1.sinaimg.cn/large/e6c9d24ely1h0ni7w3stlj20kh0i3wgt.jpg)

#### Reducer实现

![image-20220326193942858](https://tva1.sinaimg.cn/large/e6c9d24ely1h0nipcc152j20ob0eidhu.jpg)

#### 主程序

![image-20220326194110437](https://tva1.sinaimg.cn/large/e6c9d24ely1h0niqtbtfwj20o20lcdj8.jpg)
