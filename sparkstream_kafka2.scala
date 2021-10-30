import org.apache.spark.{SparkConf,SparkContext};
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml
//import java.util.Date
//import java.text.SimpleDateFormat
import org.apache.spark.sql.Row
//import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
//import org.apache.spark.sql.sources.v2
//import org.apache.spark.sql.execution.streaming.BaseStreamingSink
//import org.apache.spark.sql.streaming.DataStreamWriter

//spark-submit --class kafka_sparkstreaming --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7  --master local[4] /home/sankrrs889440/Sparkcode/project1/target/scala-2.11/testscala_2.11-2.1.1.jar
object kafka_sparkstreaming2
{
    def main(args: Array[String]) : Unit =
    {

          val spk1 = SparkSession.builder().appName("Spark SQL basics").getOrCreate()
          import spk1.implicits._
          val sp1 = spk1.sparkContext        

        val df = spk1.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "cxln4.c.thelab-240901.internal:6667")
        .option("subscribe", "ztopia1")
        .option("startingOffsets", "earliest") // From starting
        .load()

   
            val df1 = df.coalesce(1)
                     .select(split(col("value"),",").getItem(0).as("AccountID"),
                      split(col("value"),",").getItem(1).as("Tran_Amt"),
                      col("timestamp"),
                      col("topic") )
                     
            val windowedCounts = df1.groupBy(
                 window(col("timestamp"), "1 minutes", "1 minutes"),col("AccountID"))
                 .agg(sum(col("Tran_Amt")) as "SumAmnt")
                 .select("AccountID" ,"SumAmnt")
                .writeStream
                .format("console")
                //.option("path","/user/sankrrs889440/parquet")
                //.option("checkpointLocation", "/user/sankrrs889440/checkpoint")
                .outputMode("update")
                .start()
                .awaitTermination()
        
    }
    
}
    
