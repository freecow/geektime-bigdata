package com.zhanghui;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hbase列定义
 */
public class ColumnData {
    // 行键
    private byte[] rowKey;
    // 列族
    private byte[] family;
    // 属性
    private byte[] qualifier;
    // 列值
    private byte[] value;

    public ColumnData(String rowKey, String family, String qualifier, String value) {
        this.rowKey = Bytes.toBytes(rowKey);
        this.family = Bytes.toBytes(family);
        if (qualifier != null) {
            this.qualifier = Bytes.toBytes(qualifier);
        } else {
            qualifier = null;
        }
        this.value = Bytes.toBytes(value);
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public byte[] getFamily() {
        return family;
    }

    public void setFamily(byte[] family) {
        this.family = family;
    }

    public byte[] getQualifier() {
        return qualifier;
    }

    public void setQualifier(byte[] qualifier) {
        this.qualifier = qualifier;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
