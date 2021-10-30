import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
import org.apache.spark.{SparkConf,SparkContext};
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
object Spark_streaming_kafka1 {
    def main(args: Array[String]) :Unit =
    {
         val spk1 = SparkSession.builder()
                                .appName("Spark HiveQL basics")
                                .enableHiveSupport()
                                .getOrCreate()
         import spk1.implicits._
         val sc1 = spk1.sparkContext

		//val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
		 val df =   spk1
				  .readStream
				  .format("kafka")
				  .option("kafka.bootstrap.servers", "cxln4.c.thelab-240901.internal:6667")
				  .option("subscribe", "ztopia1")
				  .load()
         val df1 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
         val rdd = df1.rdd         // Writes to a single file
		  /*val ssc = new StreamingContext(sc1, Seconds(1))
		val lines = ssc.socketTextStream("cxln5",9999)
		val words = lines.flatMap(a => a.split(" ")).map(t => (t,1))
		val wordCount =words.reduceByKey(_+_)
		wordCount.print()
		ssc.start()
		ssc.awaitTermination()*/
    }
}    

