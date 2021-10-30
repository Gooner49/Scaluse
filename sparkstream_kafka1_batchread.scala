import org.apache.spark.{SparkConf,SparkContext};
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import com.databricks.spark.xml
import org.apache.spark.sql.Row
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

//spark-submit --class kafka_sparkstreaming --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7  --master local[4] /home/sankrrs889440/Sparkcode/project1/target/scala-2.11/testscala_2.11-2.1.1.jar
object kafka_batchread
{
    def main(args: Array[String]) : Unit =
    {

          val spk1 = SparkSession.builder().appName("Spark SQL basics").getOrCreate()
          import spk1.implicits._
          val sp1 = spk1.sparkContext        

        val df = spk1.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "cxln4.c.thelab-240901.internal:6667")
        .option("subscribe", "ztopia1")
        .option("startingOffsets", "earliest") // From starting
        .load()
         df.printSchema()
        val df2 = df.selectExpr("CAST(key AS STRING)", 
                                "CAST(value AS STRING)",
                                "topic")            
                                
        df2.write.mode(SaveMode.Overwrite).parquet("/user/sankrrs889440/parquet/pi1.parquet")
        
    }
    
}
    
