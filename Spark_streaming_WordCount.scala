import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
import org.apache.spark.{SparkConf,SparkContext};
import org.apache.spark.sql.SparkSession
object Spark_streaming_1 {
    def main(args: Array[String]) :Unit =
    {
         val spk1 = SparkSession.builder()
                                .appName("Spark HiveQL basics")
                                .enableHiveSupport()
                                .getOrCreate()
         import spk1.implicits._
         val sc1 = spk1.sparkContext

		//val conf = new SparkConf().setMaster("local[4]").setAppName("NetworkWordCount")
		val ssc = new StreamingContext(sc1, Seconds(1))
		val lines = ssc.socketTextStream("cxln5",9999)
		val words = lines.flatMap(a => a.split(" ")).map(t => (t,1))
		val wordCount =words.reduceByKey(_+_)
		wordCount.print()
		ssc.start()
		ssc.awaitTermination()
    }
}    

