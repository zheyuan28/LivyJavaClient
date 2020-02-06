import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Seq[String]) {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    spark.range(1, 10).show(false)
  }
}

SimpleApp.main(Seq.empty)