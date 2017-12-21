package com.kakao.hbase.tools;

import org.HdrHistogram.Histogram;
import org.apache.hadoop.hbase.util.Bytes;

// TODO : Use SynchronizedHistogram instead of Histogram under Multi-Thread
public class RowHistogram {
    byte[] maxRowKey = {};
    long currMaxValue = 0;
    Histogram histogram = null;

    byte[] globalMaxRowKey = {};
    long globalCurrMaxValue = 0;
    Histogram globalHistogram = null;

    String tableName = null;

    // getters & setters
    public byte[] getMaxRowKey() {
        return maxRowKey;
    }

    public void setMaxRowKey(byte[] maxRowKey) {
        this.maxRowKey = maxRowKey;
    }

    public long getCurrMaxValue() {
        return currMaxValue;
    }

    public void setCurrMaxValue(long currMaxValue) {
        this.currMaxValue = currMaxValue;
    }

    public byte[] getGlobalMaxRowKey() {
        return globalMaxRowKey;
    }

    public void setGlobalMaxRowKey(byte[] globalMaxRowKey) {
        this.globalMaxRowKey = globalMaxRowKey;
    }

    public long getGlobalCurrMaxValue() {
        return globalCurrMaxValue;
    }

    public void setGlobalCurrMaxValue(long globalCurrMaxValue) {
        this.globalCurrMaxValue = globalCurrMaxValue;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    // constructors
    public RowHistogram(int numberOfSignificantValueDigits) {
        this.histogram = new Histogram(numberOfSignificantValueDigits);
        this.globalHistogram = new Histogram(numberOfSignificantValueDigits);
    }

    // recordValue
    public void recordValue(byte[] rowkey, long val, String tableName) {
        this.histogram.recordValue(val);
        if (val > this.currMaxValue) {
            setCurrMaxValue(val);
            setMaxRowKey(Bytes.copy(rowkey));
        }

        this.globalHistogram.recordValue(val);
        if (val > this.globalCurrMaxValue) {
            setGlobalCurrMaxValue(val);
            setGlobalMaxRowKey(Bytes.copy(rowkey));
            setTableName(tableName);
        }
    }

    public long getMaxValue() {
        return histogram.getMaxValue();
    }

    public long getTotalCount() {
        return histogram.getTotalCount();
    }

    public long getGlobalTotalCount() {
        return globalHistogram.getTotalCount();
    }

    public void reset() {
        histogram.reset();
        maxRowKey = null;
        currMaxValue = 0;
    }
}
