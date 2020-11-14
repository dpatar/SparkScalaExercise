import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object ProductsSalesSellersExercise3 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Exercise3")
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

    val sold_pieces_bySellers = sales_ds.select("seller_id", "product_id", "num_pieces_sold")
      .withColumn("num_pieces_sold", col("num_pieces_sold").cast(IntegerType))
      .groupBy("seller_id","product_id").agg(sum("num_pieces_sold").alias("total_sold_pieces"))

    val window_desc = Window.partitionBy("product_id").orderBy(desc("total_sold_pieces"))
    val window_asc = Window.partitionBy("product_id").orderBy(asc("total_sold_pieces"))

    val sold_bySellers = sold_pieces_bySellers.withColumn("rank_asc", dense_rank.over(window_asc))
      .withColumn("rank_desc", dense_rank.over(window_desc))

    val second_seller = sold_bySellers.where($"rank_desc" === 2)
      .orderBy(desc("rank_asc")).show(5)
    val least_seller = sold_bySellers.where($"rank_asc" === 1 && $"rank_desc" =!= 1)
      .orderBy(asc("rank_desc")).show(5)


  }

}
