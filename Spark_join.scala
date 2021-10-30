import org.apache.spark.{SparkConf,SparkContext};
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.util.Calendar;
import java.text.SimpleDateFormat;
object Spark_join2 {
    def main(args: Array[String]) :Unit =
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
         val acnt_stat = sc1.textFile("/user/sankrrs889440/acnt_stat")
         val balFile = sc1.textFile("/user/sankrrs889440/acnt_bal.dat")
         val acnt_dtls_v1 = acnt_stat.map (a => a.split("\\|"))
         val balDetails = balFile.map(a => a.split("\\|"))
         val acnt_dtls_v2 =  acnt_dtls_v1.keyBy(t => t(0))
         val balDetails_v1 = balDetails.keyBy(t => t(0))
         val ks = acnt_dtls_v2.join(balDetails_v1).map(a => (a._1,a._2))
         val name_bal = ks.map( a => (a._1,a._2._1(1),a._2._2(1)))
         name_bal.saveAsTextFile("/user/sankrrs889440/name_bal")
    }
}