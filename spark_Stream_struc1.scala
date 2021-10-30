import org.apache.spark.{SparkConf,SparkContext};
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object StructStream {

    def main(args: Array[String])=
    {
         val spk1 = SparkSession.builder().appName("Spark SQL basics").getOrCreate()
         import spk1.implicits._
         val sp1 = spk1.sparkContext
         println("Hello, world")
         val lines = spk1.readStream.format("socket").option("host","cxln4").option("port",9999).load()
         val lines_split = lines.map( a => a.getAs[String](0).split(" "))
         val output_df = lines_split.select(explode($"value"))
         val output_df_count = output_df.groupBy($"col").count()
         val query = output_df_count.writeStream.outputMode("complete").format("console").start()
         query.awaitTermination()
    }
}
