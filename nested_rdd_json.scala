import org.apache.spark.{SparkConf,SparkContext};
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, ArrayType};
import org.apache.spark.sql.Row
//spark-submit --class nestedutil_json --master local[4] /home/sankrrs889440/Sparkcode/project1/target/scala-2.11/testscala_2.11-2.1.1.jar
object nestedutil_json 
{
    def main(args: Array[String])=
    {
         val spk1 = SparkSession.builder().appName("Spark SQL basics").getOrCreate()
         import spk1.implicits._
         val sc = spk1.sparkContext
         val dataSchema = StructType (
                                          Array (
                                                StructField("OfficeID",IntegerType,true),
                                                StructField("firstname",StringType,true),
                                                StructField("lastname",StringType,true),
                                                StructField("City",StringType,true)
                                                )
                                        )

         val dataSchemanstd = StructType (
                                             Array (
                                             StructField("OfficeID",IntegerType,true),
                                             StructField("tl",ArrayType(StructType(Array(
                                                                     StructField("firstname",StringType,true),
                                                                     StructField("lastname",StringType,true),
                                                                     StructField("City",StringType,true)
                                                              )))
                                                        )
                                                   )
                                        )

         val seq_data = Seq(Row(1,"James","","Morris"),
               Row(2,"David","dd","Morris"),
               Row(1,"Martha","","Mdddis"),
               Row(3,"Plic","XX","Nris"))
         val rdd_1 =  sc.parallelize(seq_data)
         val rdd_ds1 =  rdd_1.map(a => (a(0),a(1),a(2),a(3)))
//       val rdd_ds2 = rdd_ds1.groupByKey()
         val df = spk1.createDataFrame(rdd_1,dataSchema)
         val df_col1 = df.groupBy($"OfficeID").agg(collect_list(struct($"firstname",$"lastname",$"City")).as("Employees"))
         /**val rdd_cols = rdd_file.map (a => a.split(","))
         val rdd_file_cols = rdd_cols.map(a =>(a(0),(a(1),a(2))))
         val rdd_key = rdd_file_cols.groupByKey()**/
         //df_col1.write.json("/user/sankrrs889440/dataframe_nested_amount1.json")
         df_col1.write.format("json").mode("overwrite").save("/user/sankrrs889440/dataframe_nested_amount1.json")
         df_col1.printSchema
         val df_read_json = spk1.read.schema(dataSchemanstd).json("/user/sankrrs889440/dataframe_nested_amount1.json")
         df_read_json.printSchema
         //df_read_json.write.option("rootTag", "books").xml("/user/sankrrs889440/dataframe_nested_amount2.xml")
         df_read_json.write.format("com.databricks.spark.xml").option("rootTag", "OfficeInfo").save("/user/sankrrs889440/xml/dataframe_nested.xml")
    }
}
