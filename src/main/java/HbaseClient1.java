import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseClient1 {
    public static void main(String[] args) throws IOException {
        boolean kgc = exits("kgc");
        System.out.println(kgc);
    }
    //全局对象
    public static Connection conn = null;
    //静态代码块初始化
    static {
        //获取conf连接
        HBaseConfiguration conf = new HBaseConfiguration();
        conf.set("hbase.zookeeper.quorum","192.168.184.100");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        //获取连接对象

        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //判断表是否存在
    public static boolean exits(String table) throws IOException {
        //拿到admin总指挥的对象
        Admin admin = conn.getAdmin();
        boolean b = admin.tableExists(TableName.valueOf(table));
        return b;
    }
    //创建表
    public static void createTable(String table,String...cf) throws IOException {
        Admin admin = conn.getAdmin();
        HTableDescriptor hTableDescriptor = new HTableDescriptor(table);
        //遍历cf
        for (String s:cf) {
            hTableDescriptor.addFamily(new HColumnDescriptor(s));
        }
        admin.createTable(hTableDescriptor);
        System.out.println("创建成功!");
    }
    //删除表
    public static void deleteTable(String table) throws IOException {
        Admin admin = conn.getAdmin();
        admin.disableTable(TableName.valueOf(table));
        if (exits(table)) {
            admin.deleteTable(TableName.valueOf(table));
            System.out.println("删除成功！");
        }else {
            System.out.println("表不存在！");
        }
    }
    //添加数据
    public static void addTable(String table,String rowkey,String columnfamily,String column,String value) throws IOException {
        Table table1 = conn.getTable(TableName.valueOf(table));
        //new put对象上传数据
        Put put = new Put(Bytes.toBytes(rowkey));
        put.add(Bytes.toBytes(columnfamily),Bytes.toBytes(column),Bytes.toBytes(value));
        //告知连接对象put的值
        table1.put(put);
        System.out.println("添加成功！");

    }
    //查看表scan
    public static void showTable(String table) throws IOException {
        Table table1 = conn.getTable(TableName.valueOf(table));
        Scan scan = new Scan();
        ResultScanner results = table1.getScanner(scan);
        for (Result result : results) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println(new String(cell.getRow()));
                System.out.println(cell.getFamily().toString());
                System.out.println(cell.getQualifier().toString());
                System.out.println(cell.getValue().toString());
            }
        }
    }
    //查看get
    public static void getTable(String table) throws IOException {
        Table table1 = conn.getTable(TableName.valueOf(table));
        Get get = new Get(Bytes.toBytes(table));
        Result result = table1.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(cell.getRow().toString());
            System.out.println(cell.getFamily().toString());
            System.out.println(cell.getQualifier().toString());
            System.out.println(cell.getValue().toString());
        }
    }
}
