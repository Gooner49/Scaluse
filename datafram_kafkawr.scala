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
object kafka_dfwrite
{
    def main(args: Array[String])=
    {


          val spk1 = SparkSession.builder()
                                .appName("Spark HiveQL basics")
                                .enableHiveSupport()
                                .getOrCreate()
         import spk1.implicits._
         val sc1 = spk1.sparkContext
         val schemaType_file = new StructType()
                              .add("ID", IntegerType)
                              .add("Name", StringType)
                              //.add("Balance", DoubleType)

         val acnt_stat = sc1.textFile("/user/sankrrs889440/acnt_stat").toDF()
   } 
}
    
