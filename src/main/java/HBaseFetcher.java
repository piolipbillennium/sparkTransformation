import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class HBaseFetcher {
    private static SQLContext sql;

    public static void main(String[] args) throws IOException {
        Properties properties = new Properties();
        JavaHBaseContext hBaseContext;
        Configuration config = HBaseConfiguration.create();

        FileInputStream fileInputStream = new FileInputStream("config.properties");
        properties.load(fileInputStream);
        fileInputStream.close();
        Connection connection = ConnectionFactory.createConnection(config);
        Admin admin = connection.getAdmin();

        SparkSession sparkSession = SparkSession.builder()
                .appName("hsr")
                .getOrCreate();

        SparkContext sc = sparkSession.sparkContext();
        JavaSparkContext jsc = new JavaSparkContext(sc);
        config.addResource(new Path(properties.getProperty("hbase.site.path")));

        hBaseContext = new JavaHBaseContext(jsc, config);
        /*Dataset<Row> df1;


        df1 = sparkSession.read()
                .format("org.apache.hadoop.hbase.spark")
                .option("hbase.columns.mapping",
                        "ROW String :key, DESCRIPTIONS String DESCRIPTIONS:CityDescription")
                .option("hbase.table", "TOURISM:CITIESDESCRIPTION")
                .option("hbase.spark.use.hbasecontext", false)
                //.option("hbase.config.resources", "file:///etc/hadoop/conf/hdfs-site.xml")
                .load();
        df1.createOrReplaceTempView("personView");
        df1.printSchema();
        df1.show();*/

        System.out.println("11111111111111111111111111111111111111");
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();

        Dataset<Row> df2;
        df2 = sparkSession.read()
                .format("org.apache.hadoop.hbase.spark")
                .option("hbase.columns.mapping",
                        "ROW String :key, RAW_DATA_ID String RAW_DATA_ID:city_name" +
                        "ROW Integer :key, RAW_DATA_ID String RAW_DATA_ID:longitude" +
                        "ROW Integer :key, RAW_DATA_ID String RAW_DATA_ID:latitude")
                .option("hbase.table", "HOTEL_FETCHER_RAW_DATA")
                .option("hbase.spark.use.hbasecontext", false)
                //.option("hbase.config.resources", "file:///etc/hadoop/conf/hdfs-site.xml")
                .load();


        df2.printSchema();
        df2.show();

        }
}