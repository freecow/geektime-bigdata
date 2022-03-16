package com.zhanghui;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

//import javax.ws.rs.GET;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestHbase {
    private Configuration conf = null;
    private Connection conn = null;
    private Table table = null;

    /**
     * 与HBASE建立连接
     * 
     * @throws IOException
     */
    private void createConf() throws IOException {
        conf = HBaseConfiguration.create();
        // 设置ZK集群地址，默认2181端口，可以不设置
        conf.set("hbase.zookeeper.quorum",
                "emr-header-1.cluster-285604,emr-worker-1.cluster-285604,emr-worker-2.cluster-285604");
        // 建立连接
        conn = ConnectionFactory.createConnection(conf);

    }

    /**
     *
     * @param tableName  表名
     * @param columnData 列字段
     * @throws IOException
     */
    private void insertData(String tableName, ColumnData columnData) throws IOException {
        table = conn.getTable(TableName.valueOf(tableName));
        // 创建put对象
        Put put = new Put(columnData.getRowKey());
        // 添加一行列数据
        put.addColumn(columnData.getFamily(), columnData.getQualifier(), columnData.getValue());
        // put.addColumn("name".getBytes(),null,"jack".getBytes());
        table.put(put);
        System.out.println("插入数据完成");
    }

    /**
     *
     * @param tableName   表名
     * @param columnDatas 批量数据
     * @throws IOException
     */
    private void batchInsrtDatas(String tableName, List<ColumnData> columnDatas) throws IOException {
        table = conn.getTable(TableName.valueOf(tableName));
        for (ColumnData columnData : columnDatas) {
            // 创建put对象
            Put put = new Put(columnData.getRowKey());
            // 添加一行列数据
            put.addColumn(columnData.getFamily(), columnData.getQualifier(), columnData.getValue());
            // put.addColumn("name".getBytes(),null,"jack".getBytes());
            table.put(put);
        }
        System.out.println("批量数据插入完成");

    }

    /**
     * 根据键值查询单条数据
     * 
     * @param tableName 表名
     * @param rowKey    行键值
     */
    private void getDataBykey(String tableName, String rowKey) throws IOException {
        table = conn.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        byte[] name = result.getValue(Bytes.toBytes("name"), null);
        byte[] student_id = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("student_id"));
        byte[] clazz = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("class"));
        byte[] understanding = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("understanding"));
        byte[] programming = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("programming"));
        String nameStr = null;
        String student_id_Str = null;
        String clazzStr = null;
        String understandingStr = null;
        String programmingStr = null;
        if (name != null) {
            nameStr = Bytes.toString(name);
        }
        if (student_id != null) {
            student_id_Str = Bytes.toString(student_id);
        }
        if (clazz != null) {
            clazzStr = Bytes.toString(clazz);
        }
        if (understanding != null) {
            understandingStr = Bytes.toString(understanding);
        }
        if (programming != null) {
            programmingStr = Bytes.toString(programming);
        }

        System.out.println("查询到结果，行键值:" + rowKey + ",name:" + nameStr + ",student_id:" + student_id_Str + ",class:"
                + clazzStr + ",understanding:" + understandingStr + ",programming:" + programmingStr);
    }

    /**
     * 根据列值查询数据
     * 
     * @param tableName  表名
     * @param columnData 列数据
     */
    private void getDataByColumn(String tableName, ColumnData columnData) throws IOException {
        table = conn.getTable(TableName.valueOf(tableName));
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(columnData.getFamily(),
                columnData.getQualifier(), CompareFilter.CompareOp.EQUAL, columnData.getValue());
        Scan scan = new Scan();
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        printResult(scanner);
    }

    /**
     * 根据键值删除数据
     * 
     * @param tableName 表名
     * @param rowKey    行键值
     */

    private void delDataByKey(String tableName, String rowKey) throws IOException {
        table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        System.out.println("删除数据成功,键值为:" + rowKey);

    }

    /**
     * 根据列值删除数据
     * 
     * @param tableName  表名
     * @param rowKey     表名 行键值
     * @param columnData 列数据
     */
    private void delDataByColumn(String tableName, String rowKey, ColumnData columnData) throws IOException {
        table = conn.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(columnData.getFamily(), columnData.getQualifier());
        table.delete(delete);
        System.out.println("删除数据成功,键值为:" + rowKey + ",Family:" + Bytes.toString(columnData.getFamily()) + ",Qualifier:"
                + Bytes.toString(columnData.getQualifier()));
    }

    private void printResult(ResultScanner scanner) {
        for (Result result : scanner) {
            byte[] rowKey = result.getRow();
            byte[] name = result.getValue(Bytes.toBytes("name"), null);
            byte[] student_id = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("student_id"));
            byte[] clazz = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("class"));
            byte[] understanding = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("understanding"));
            byte[] programming = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("programming"));
            String rowKeyStr = null;
            String nameStr = null;
            String student_id_Str = null;
            String clazzStr = null;
            String understandingStr = null;
            String programmingStr = null;
            rowKeyStr = Bytes.toString(rowKey);
            if (name != null) {
                nameStr = Bytes.toString(name);
            }
            if (student_id != null) {
                student_id_Str = Bytes.toString(student_id);
            }
            if (clazz != null) {
                clazzStr = Bytes.toString(clazz);
            }
            if (understanding != null) {
                understandingStr = Bytes.toString(understanding);
            }
            if (programming != null) {
                programmingStr = Bytes.toString(programming);
            }
            System.out.println("查询到结果，行键值:" + rowKeyStr + ",name:" + nameStr + ",student_id:" + student_id_Str
                    + ",class:" + clazzStr + ",understanding:" + understandingStr + ",programming:" + programmingStr);
        }

    }

    public static void main(String[] args) throws IOException {
        TestHbase testHbase = new TestHbase();
        testHbase.createConf();
        // ColumnData columnData =new ColumnData("20210000000003","info","class","2");
        // testHbase.insertData("gaoyong:student",columnData);
        ColumnData row1_columnData1 = new ColumnData("20210000000002", "name", null, "Tom");
        ColumnData row1_columnData2 = new ColumnData("20210000000002", "info", "student_id", "20210000000001");
        ColumnData row1_columnData3 = new ColumnData("20210000000002", "info", "class", "1");
        ColumnData row1_columnData4 = new ColumnData("20210000000002", "score", "understanding", "75");
        ColumnData row1_columnData5 = new ColumnData("20210000000002", "score", "programming", "82");

        ColumnData row1_columnData1 = new ColumnData("20210000000002", "name", null, "Jerry");
        ColumnData row1_columnData2 = new ColumnData("20210000000002", "info", "student_id", "20210000000002");
        ColumnData row1_columnData3 = new ColumnData("20210000000002", "info", "class", "1");
        ColumnData row1_columnData4 = new ColumnData("20210000000002", "score", "understanding", "85");
        ColumnData row1_columnData5 = new ColumnData("20210000000002", "score", "programming", "67");

        ColumnData row2_columnData1 = new ColumnData("20210000000003", "name", null, "Jack");
        ColumnData row2_columnData2 = new ColumnData("20210000000003", "info", "student_id", "20210000000003");
        ColumnData row2_columnData3 = new ColumnData("20210000000003", "info", "class", "2");
        ColumnData row2_columnData4 = new ColumnData("20210000000003", "score", "understanding", "80");
        ColumnData row2_columnData5 = new ColumnData("20210000000003", "score", "programming", "80");

        ColumnData row3_columnData1 = new ColumnData("20210000000004", "name", null, "Rose");
        ColumnData row3_columnData2 = new ColumnData("20210000000004", "info", "student_id", "20210000000004");
        ColumnData row3_columnData3 = new ColumnData("20210000000004", "info", "class", "2");
        ColumnData row3_columnData4 = new ColumnData("20210000000004", "score", "understanding", "60");
        ColumnData row3_columnData5 = new ColumnData("20210000000004", "score", "programming", "61");

        ColumnData row4_columnData1 = new ColumnData("G20200388040069", "name", null, "张煇");
        ColumnData row4_columnData2 = new ColumnData("G20200388040069", "info", "student_id", "G20210607040077");
        ColumnData row4_columnData3 = new ColumnData("G20200388040069", "info", "class", "1");
        ColumnData row4_columnData4 = new ColumnData("G20200388040069", "score", "understanding", "81");
        ColumnData row4_columnData5 = new ColumnData("G20200388040069", "score", "programming", "82");

        List<ColumnData> columnDatas = new ArrayList<ColumnData>();
        columnDatas.add(row1_columnData1);
        columnDatas.add(row1_columnData2);
        columnDatas.add(row1_columnData3);
        columnDatas.add(row1_columnData4);
        columnDatas.add(row1_columnData5);

        columnDatas.add(row2_columnData1);
        columnDatas.add(row2_columnData2);
        columnDatas.add(row2_columnData3);
        columnDatas.add(row2_columnData4);
        columnDatas.add(row2_columnData5);

        columnDatas.add(row3_columnData1);
        columnDatas.add(row3_columnData2);
        columnDatas.add(row3_columnData3);
        columnDatas.add(row3_columnData4);
        columnDatas.add(row3_columnData5);

        columnDatas.add(row4_columnData1);
        columnDatas.add(row4_columnData2);
        columnDatas.add(row4_columnData3);
        columnDatas.add(row4_columnData4);
        columnDatas.add(row4_columnData5);

        testHbase.batchInsrtDatas("zhanghui:student", columnDatas);
        // testHbase.getDataBykey("zhanghui:student","20210000000001");
        /*
         * ColumnData columnData =new ColumnData("20210000000003","name",null,"Tom");
         * testHbase.getDataByColumn("zhanghui:student",columnData);
         */
        // testHbase.delDataByKey("zhanghui:student","20210000000001");
        /*
         * ColumnData columnData =new
         * ColumnData("20210000000003","score","understanding","80");
         * testHbase.delDataByColumn("zhanghui:student","20210000000003",columnData);
         */
    }
}
