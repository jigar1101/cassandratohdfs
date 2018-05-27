import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;
import java.util.HashMap;

public class CassandraToHDFS {

    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf(true);

        conf.set("spark.cassandra.connection.host", "localhost");

        conf.set("spark.cassandra.connection.port", "9042");


        conf.setAppName("CassandraToHDFS");

        // Setting master to run locally with 2 threads
        // This can be also be set to run with yarn cluster in production mode and arguments can
        // be passed using the spark submit command
        conf.setMaster("local[2]");

        SparkContext context = new SparkContext(conf);
        SQLContext sqlContext = new SQLContext(context);

        DataFrame cassandraDataFrame = sqlContext.read().format("org.apache.spark.sql.cassandra")
                .options(new HashMap<String, String>() {
                    {
                        put("keyspace", "demo");
                        put("table", "emp");
                        put("pushdown", "true");
                    }
                }).load();

        //Explains the Query
        cassandraDataFrame.explain();

        //Stores the dataframe data into temp table
        cassandraDataFrame.registerTempTable("data");

        DataFrame result = sqlContext.sql("select count(emp_id) as count, emp_sal from data group by emp_sal order by count desc");

        result.show();

        //Replace path with hdfs path in case of storing in HDFS
        result.coalesce(1).write().format("json").mode(SaveMode.Overwrite).save("/tmp/opensource");

    }
}
