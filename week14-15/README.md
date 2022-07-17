# 作业：Flink

## 作业要求

report(transactions).executeInsert(“spend_report”);
将 transactions 表经过 report 函数处理后写入到 spend_report 表。

每分钟（或小时）计算在五分钟（或小时）内每个账号的平均交易金额（滑动窗口）？使用分钟还是小时作为单位均可。

## 步骤说明

### 克隆代码

```xml
git clone  https://github.com/apache/flink-playgrounds.git
```
### 实现方法report

找到修改flink-playground/table-walkthrough中的SpendReport

```
public class SpendReport {
    public static Table report(Table transactions) {
        return transactions
                .window(Slide.over(lit(5).minutes())
                        .every(lit(1).minutes())
                        .on($("transaction_time"))
                        .as("window")
                )
                .groupBy($("account_id"), $("window"))
                .select(
                        $("account_id"),
                        $("window").start().as("log_ts"),
                        $("amount").avg().as("amount"));
    }
}
```

### 构建镜像

```bash
cd flink-playgrounds/table-walkthrough
docker-compose build
```

### 启动容器

```scala
docker-compose up -d
```
### 查看验证Flink

打开浏览器并访问 http://localhost:8081

### 查看日志

查看JobManager日志：`docker-compose logs -f jobmanager`

查看 TaskManager 日志：`docker-compose logs -f taskmanager`

### 结果导入MySQL

```
ocker-compose exec mysql mysql -Dsql-demo -usql-demo -pdemo-sql
```

### MySQL命令行查看结果

```bash
use sql-demo;

select count(*) from spend_report;
```

