package com.kakao.hbase.tools;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.PropertyConfigurator;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.kakao.hbase.tools.Util.*;

public class WideRowPicker {
    private Admin admin;

    private RowHistogram keySizeHistogram = new RowHistogram(3);
    private RowHistogram rowSizeHistogram = new RowHistogram(3);
    private RowHistogram columnCountPerRowHistogram = new RowHistogram(3);
    private RowHistogram valueSizeHistogram = new RowHistogram(3);
    private RowHistogram tombstoneCountHistogram = new RowHistogram(3);
    private RowHistogram tombstoneHistogram = new RowHistogram(3);
    private RowHistogram tombKeySizeHistogram = new RowHistogram(3);            // tomb rowkey size

    private final Connection conn;

    public WideRowPicker(Connection conn) throws IOException {
        this.conn = conn;
        this.admin = conn.getAdmin();
        System.out.println("conn.getAdmin() : " + this.admin);
    }

    private void printTableHistogram(RowHistogram rowSizeHistogram, RowHistogram tombstoneHistogram) {
        String rowkey = Bytes.toStringBinary(rowSizeHistogram.getMaxRowKey());
        String tombRowkey = Bytes.toStringBinary(tombstoneHistogram.getMaxRowKey());

        if (rowkey.equals("null")) return;

        Util.println(SEPARATOR2);
        Util.println("* max row size (bytes) : " + rowSizeHistogram.getMaxValue());
        Util.println("  -rowkey : " + rowkey);
        Util.println("* max rowkey size : " + keySizeHistogram.getMaxValue());
        Util.println("* max value size : " + valueSizeHistogram.getMaxValue());

        Util.println("* max column count : " + columnCountPerRowHistogram.getMaxValue());
        Util.println("  -rowkey : " + Bytes.toStringBinary(columnCountPerRowHistogram.getMaxRowKey()));
        Util.println("* row count (+tomb) : " + rowSizeHistogram.getTotalCount());

        Util.println("\n* [tomb] max row's rowkey : " + tombRowkey);
        Util.println("* [tomb] row count : " + tombstoneCountHistogram.getTotalCount());
        Util.println(SEPARATOR2);
    }

    private void printRegionHistogram(RowHistogram rowSizeHistogram, RowHistogram tombstoneHistogram) {
        Util.println("\n\n");
        Util.println(SEPARATOR);
        Util.println("\t\tTOTAL RESULT");
        Util.println(SEPARATOR);
        Util.println("* max row size (bytes) : " + rowSizeHistogram.getGlobalCurrMaxValue());
        Util.println("  -rowkey : " + Bytes.toStringBinary(rowSizeHistogram.getGlobalMaxRowKey()));
        Util.println("  -region name : " + rowSizeHistogram.getTableName());

        Util.println("\n* max column count : " + columnCountPerRowHistogram.getGlobalCurrMaxValue());
        Util.println("  -region name : " + columnCountPerRowHistogram.getTableName());
        Util.println("  -rowkey : " + Bytes.toStringBinary(columnCountPerRowHistogram.getGlobalMaxRowKey()));

        Util.println("\n* row count (+tomb) : " + rowSizeHistogram.getGlobalTotalCount());

        Util.println("\n* [tomb] max row size (bytes) : " + tombstoneHistogram.getGlobalCurrMaxValue());
        Util.println("  -region name : " + tombstoneHistogram.getTableName());
        Util.println("  -rowkey : " + Bytes.toStringBinary(tombstoneHistogram.getGlobalMaxRowKey()));
        Util.println("  -row count : " +tombstoneCountHistogram.getGlobalTotalCount());
        Util.println(SEPARATOR);
    }

    public void scan(String regionServer, String args_table, String args_limit) throws IOException {
        String pattern = (args_table.length() > 0) ? args_table : "(SYSTEM|meta).*";
        args_limit = (args_limit.length() > 0) ? args_limit : "10000";

        ClusterStatus clStatus = admin.getClusterStatus();
        for (ServerName serverName : clStatus.getServers()) {       // line_a
            if (!regionServer.equals(serverName.getHostname())) continue;
        /* comment this line, line_a and line_b if you are using JDK8
        ServerName serverName = clStatus.getServers().stream()
                                .filter((x) -> x.getHostname().equals(regionServer))
                                .findFirst()
                                .get();
        /**/
            Util.println(SEPARATOR2);
            Util.println("* Region Server : " + serverName.getServerName());
            Util.println("* table name : " + args_table);
            Util.println("* scan limit : " + args_limit);
            Util.println(SEPARATOR);

            List<HRegionInfo> online = admin.getOnlineRegions(serverName);
            for (HRegionInfo r : online) {
                byte[] startKey = r.getStartKey();
                byte[] endKey = r.getEndKey();

                String encodedRegionName = r.getRegionNameAsString();
                String namespace = "";
                String tableName = "";
                String[] temp = r.getTable().toString().split(":");

                if (temp.length > 1) {
                    namespace = temp[0];
                    tableName = temp[1];
                } else if (temp.length == 1) {
                    namespace = "default";
                    tableName = temp[0];
                }

                if ((args_table.length() > 0) && Pattern.matches(pattern, tableName)) {
                    regionScan(startKey, endKey, encodedRegionName, namespace, tableName, args_limit);
                } else if ((args_table.length() == 0) && (!Pattern.matches(pattern, tableName))) {
                    regionScan(startKey, endKey, encodedRegionName, namespace, tableName, args_limit);
                }
            }
            printRegionHistogram(rowSizeHistogram, tombstoneHistogram);
        }                                                           // line_b
    }

    private void regionScan(byte[] startKey, byte[] endKey, String encodedRegionName,
                            String namespace, String tablename, String args_limit)
                throws IOException {
        TableName tableName;
        Util.println("\n\n");
        Util.println(SEPARATOR2);
        Util.println("# encoded region name : " +encodedRegionName);
        Util.println("  -start key, end key : " + Bytes.toStringBinary(startKey) + ", " + Bytes.toStringBinary(endKey));

        tableName = TableName.valueOf(namespace + ":" + tablename);
        Table table = conn.getTable(tableName);

        Scan scan = new Scan();
        scan.setStartRow(startKey);
        scan.setStopRow(endKey);
        scan.setRaw(true);
        scan.setCacheBlocks(false);

        ResultScanner resultScan = table.getScanner(scan);
        byte[] row;

        keySizeHistogram.reset();
        rowSizeHistogram.reset();
        columnCountPerRowHistogram.reset();
        valueSizeHistogram.reset();
        tombstoneCountHistogram.reset();
        tombstoneHistogram.reset();

        for (Result result : resultScan) {
            row = result.getRow();
            List<Cell> cells = result.listCells();

            // key length
            keySizeHistogram.recordValue(row, row.length, encodedRegionName);

            // row's column count
            columnCountPerRowHistogram.recordValue(row, cells.size(), encodedRegionName);

            int tombstoneCount = 0;
            for (Cell cell : result.listCells()) {

                int rowSizeInBytes = 0;
                int tombstoneSizeInBytes = 0;

                byte[] sourceRowkey = CellUtil.cloneRow(cell);
                rowSizeInBytes += sourceRowkey.length;
                byte[] family = CellUtil.cloneFamily(cell);
                rowSizeInBytes += family.length;
                byte[] col = CellUtil.cloneQualifier(cell);
                rowSizeInBytes += col.length;
                byte[] value = CellUtil.cloneValue(cell);
                rowSizeInBytes += value.length;

                if (CellUtil.isDelete(cell)) { // tombstone
                    // [tomb] row count
                    tombstoneCountHistogram.recordValue(row, ++tombstoneCount, encodedRegionName);

                    // [tomb] row size (=rowkey+cf+col+value) in Bytes
                    tombstoneSizeInBytes += rowSizeInBytes;
                    tombstoneHistogram.recordValue(row, tombstoneSizeInBytes, encodedRegionName);
                } else {
                    // value size
                    valueSizeHistogram.recordValue(row, value.length, encodedRegionName);
                    // row size (=rowkey+cf+col+value) in Bytes
                    rowSizeHistogram.recordValue(row, rowSizeInBytes, encodedRegionName);
                }
            }

            if (( Integer.parseInt(args_limit) > 0) && (rowSizeHistogram.getTotalCount() >= Integer.parseInt(args_limit)) ) {
                printTableHistogram(rowSizeHistogram, tombstoneHistogram);
                return;
            }

            if (rowSizeHistogram.getTotalCount() % 5000000==0)
                printTableHistogram(rowSizeHistogram, tombstoneHistogram);
        }
        printTableHistogram(rowSizeHistogram, tombstoneHistogram);
    }

    public static void main(String[] args) throws IOException, LoginException, InterruptedException {
        Args argsObj = new Args(args);

        String zk = argsObj.getZK();
        String rs = argsObj.getRS();

        String jaas = argsObj.getJaas();
        String krb5 = argsObj.getKrb5();
        String realm = argsObj.getRealm();

        String args_table = argsObj.getTable();
        String args_limit = argsObj.getLimit();

        Properties props = new Properties();
        props.setProperty("log4j.threshold", "ERROR");
        PropertyConfigurator.configure(props);

        HBaseConnector hConn = new HBaseConnector();

        WideRowPicker wideRowPicker = (jaas.length() != 0) ?
                new WideRowPicker(hConn.gethConn(zk, jaas, krb5, realm)) :
                new WideRowPicker(hConn.gethConn(zk));

        wideRowPicker.scan(rs, args_table, args_limit);
    }
}