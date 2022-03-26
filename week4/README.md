### 数据说明

共有3份数据，分别是users.dat、movies.dat、ratings.dat，movies.dat
```bash
# users.dat
数据格式：6040::M::25::6::11106
对应字段：user_id bigint、sex string、age int、occupation int、zipcode bigint
具体含义：用户ID、性别、年龄、职业、邮编

# movies.dat
数据格式：3952::Contender, The (2000)::Drama|Thriller
对应字段：movie_id bigint、movie_name string、movie_type string
具体含义：影片ID、电影名、电影类型

# ratings.dat
数据格式：6040::1097::4::956715569
对应字段：user_id bigint、movie_id bigint、rate tinyint、times bigint
具体含义：用户ID、影片ID、评分、评分时间戳
```

### 导入hdfs数据
```bash
# hdfs分区创建自己的数据目录
hadoop fs -mkdir -p /user/zhanghui/movies
hadoop fs -mkdir -p /user/zhanghui/ratings
hadoop fs -mkdir -p /user/zhanghui/users

# 复制共享区的hdfs文件到自己的数据目录
hadoop fs -cp /data/hive/movies/*  /user/zhanghui/movies
hadoop fs -cp /data/hive/ratings/*  /user/zhanghui/ratings
hadoop fs -cp /data/hive/users/*  /user/zhanghui/users
hadoop fs -ls /user/zhanghui/movies
hadoop fs -ls /user/zhanghui/ratings
hadoop fs -ls /user/zhanghui/users
```

### 创建Hive数据库及表
```bash
# 访问hive
beeline -u jdbc:hive2://localhost:10000

# 检查数据库
show databases;

# 创建数据库
create database if not exists zhanghui comment "student zhanghui";

# 创建表h_movie
create external table if not exists zhanghui.h_movie(
movie_id bigint comment "影片ID",
movie_name string comment "电影名",
movie_type string comment "电影类型"
)
comment "电影"
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ("field.delim"="::")
stored as textfile;


# 创建表h_rating
create external table if not exists zhanghui.h_rating(
user_id bigint comment "用户ID",
movie_id bigint comment "影片ID",
rate tinyint comment "ID",
times bigint comment "影评次数"
)
comment "评价"
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ("field.delim"="::")
stored as textfile;


# 创建表h_user
create table if not exists zhanghui.h_user(
user_id bigint comment "用户ID",
sex string comment "性别",
age int comment "年龄",
occupation int comment "职业",
zipcode bigint comment "邮编"
)
comment "用户"
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' 
WITH SERDEPROPERTIES ("field.delim"="::")
stored as textfile;
```

### 导入hdfs数据
```bash
# 装入数据
load data inpath '/user/zhanghui/movies/movies.dat' OVERWRITE into table zhanghui.h_movie;
load data inpath '/user/zhanghui/ratings/ratings.dat' OVERWRITE into table zhanghui.h_rating;
load data inpath '/user/zhanghui/users/users.dat' OVERWRITE into table zhanghui.h_user;

# 显示已创建表
use zhanghui;
show tables;

# 显示表结构
desc t_movie;

# 显示表创建语句
show create table t_movie;

# 显示表内记录
select * from t_movie;
```

### 题目 1

展示电影 ID 为 2116 这部电影各年龄段的平均影评分

**解答：**
```bash
# 结果放入表q1
create table q1 as
select u.age as age,avg(r.rate) as avgrate
from h_rating r join h_user u on r.user_id=u.user_id
where r.movie_id=2116
group by u.age;

# 显示q1记录
select * from q1;
```

**运行结果：**



![q1](https://tva1.sinaimg.cn/large/e6c9d24ely1h0lgohwxikj20n90mjdkr.jpg)



### 题目 2

找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数

**解答：**
```bash
# 结果放入q2
create table q2 as 
select "M" as sex, m.movie_name as name, avg(r.rate) as avgrate, count(m.movie_name) as total  
from h_rating r 
join h_user u on r.user_id=u.user_id 
join h_movie m on r.movie_id=m.movie_id 
where u.sex="M" 
group by m.movie_name 
having total >= 50
order by avgrate desc 
limit 10;

# 显示q2表记录
select * from q2;
```

**运行结果：**



![q2](https://tva1.sinaimg.cn/large/e6c9d24ely1h0lgow34jej21h80saqdc.jpg)



### 题目 3

找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分

**解答：**

第一步 先求出最喜欢看电影的那位女性
```bash
select r.user_id, count(r.user_id) as total 
from h_rating r join h_user u on r.user_id = u.user_id 
where u.sex="F" 
group by r.user_id 
order by total desc 
limit 1;
```
运行结果：



![s1](https://tva1.sinaimg.cn/large/e6c9d24ely1h0lgp62oi9j21460bognb.jpg)

第二步 根据第一步中求出的女性user_id作为where过滤条件，以其看过的电影评分rate作为排序条件进行排序，求出评分最高的10部电影
```bash
create table q3a as 
select r.movie_id as movie_id, r.rate as rate  
from h_rating r 
where r.user_id=1150 
order by rate desc 
limit 10;

select * from q3a;
```
运行结果：



![s2](https://tva1.sinaimg.cn/large/e6c9d24ely1h0lgpemtj0j21480k0q5b.jpg)

第三步 求出第二步中得出的10部电影的平均电影评分
```bash
# 运行结果存入表q3b
create table q3b as 
select r.movie_id as movie_id, m.movie_name as movie_name, avg(r.rate) as avgrate 
from q3a qa 
join h_rating r on qa.movie_id=r.movie_id 
join h_movie m on r.movie_id=m.movie_id 
group by r.movie_id,m.movie_name;

# 显示表q3b的记录
select * from q3b;
```
运行结果：



![s3](https://tva1.sinaimg.cn/large/e6c9d24ely1h0lgplnd9bj21im0i4aeq.jpg)
