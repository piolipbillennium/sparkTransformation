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
import scala.collection.Seq;
import static org.apache.spark.sql.functions.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
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

        Dataset<Row> df2 = sparkSession.read()
                .format("org.apache.hadoop.hbase.spark")
                .option("hbase.columns.mapping",
                        "ROW String :key, " +
                                "RAW_DATA_ID_city_name String RAW_DATA_ID:city_name," +
                                "RAW_DATA_ID_longitude String RAW_DATA_ID:longitude," +
                                "RAW_DATA_ID_latitude String RAW_DATA_ID:latitude," +
                                "RAW_DATA_api_raw_data String RAW_DATA:api_raw_data")
                .option("hbase.table", "HOTEL_FETCHER_RAW_DATA")
                .option("hbase.spark.use.hbasecontext", false)
                .load();
        df2.createOrReplaceTempView("personView");
        //df2.printSchema();

        df2.show();
        //df2.select("RAW_DATA_444").show();
        System.out.println("1");
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        df2.getRows(1, 1);
        System.out.println("2");
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println(df2.getRows(1, 1));
        System.out.println("3");
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println(df2.getRows(1, 1).getClass());
        System.out.println("4");
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println(df2.getRows(1, 1).mkString());
        System.out.println("5");
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println(Arrays.toString(new Seq[]{df2.getRows(1, 1)}));
        System.out.println("5");
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();

        //df2.select(col("RAW_DATA_333"), wheren(col("RAW_DATA_111").equalTo("London"))).show();


        /*sparkSession.sql("select RAW_DATA_333, RAW_DATA_111 " +
                                "FROM personView " +
                                "WHERE RAW_DATA_111 = 'London';").show();*/
        System.out.println();
        System.out.println();
        System.out.println();
        System.out.println();
        //df2.select("RAW_DATA_333")
        //System.out.println(df2.select(col("RAW_DATA_333"), when(col("RAW_DATA_111"). equalTo("London")),col("enabled")));
        System.out.println("5");
        //System.out.println(sparkSession.sql("select id,name,color, case when color = 'red' and price is not null then (price + 2.55) when color = 'red' and price is null then 2.55 else price end as price, enabled from df").show(););
        System.out.println();
        System.out.println();
        System.out.println();







        }
}