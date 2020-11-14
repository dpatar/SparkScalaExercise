import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


object ProductsSalesSellersExercise2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .master("local[*]").config("spark.executor.memory", "500mb")
      .appName("Exercise2")
      .getOrCreate()


    import spark.implicits._
    val products_ds = spark.read
      .option("header",true)
      .option("inferSchema", true)
      .parquet("data/products_parquet")

    val sales_ds = spark.read
      .option("inferSchema", true)
      .option("header",true)
      .parquet("data/sales_parquet")

    val sellers_ds = spark.read
      .option("inferSchema", true)
      .option("header",true)
      .parquet("data/sellers_parquet")

    sales_ds.select("seller_id", "num_pieces_sold")
      .withColumn("num_pieces_sold", col("num_pieces_sold").cast(IntegerType))
      .groupBy("seller_id").agg(avg("num_pieces_sold").alias("avr_sold_daily"))
      .orderBy(desc("seller_id"))
      .join(broadcast(sellers_ds.select("seller_id","daily_target")), "seller_id")
      .withColumn("daily_target", col("daily_target").cast(IntegerType))
      .withColumn("ratio", $"avr_sold_daily"/$"daily_target")
      .orderBy(desc("seller_id")).show(10)

  }

}
