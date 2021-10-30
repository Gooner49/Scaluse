import org.apache.spark.{SparkConf,SparkContext};
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, ArrayType};
import org.apache.spark.sql.Row
import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
//spark-submit --class kafkabasics --master local[4] /home/sankrrs889440/Sparkcode/project1/target/scala-2.11/testscala_2.11-2.1.1.jar
object kafkabasics
{
    def main(args: Array[String])=
    {

          val props:Properties = new Properties()
          props.put("bootstrap.servers","cxln4.c.thelab-240901.internal:6667")
          props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
          props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
          props.put("acks","all")
          val producer = new KafkaProducer[String, String](props)
          val topic = "ztopia1"
          var i =0
          try {
          for (i <- 0 to 15) {
                  val record = new ProducerRecord[String, String](topic, i.toString, "My Site is sparkbyexamples.com " + i)
                  val metadata = producer.send(record)
                  printf(s"sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d)\n", record.key(), record.value(), metadata.get().partition(),metadata.get().offset())

              }
          }
          catch{
                 case e:Exception => e.printStackTrace()
               }
         finally {
                    producer.close()
                }
    }
    
}
    
