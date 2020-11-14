import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType,StringType,StructType,FloatType,DateType, LongType}
import org.apache.spark.sql.functions._
import org.apache.log4j._


object ProductsSalesSellersExercise1 {

  def saltedJoin(df: DataFrame, buildDf: DataFrame, joinExpression: Column,
                 joinType: String, salt: Int): DataFrame = {
    val tmpDf = buildDf.withColumn("salt_range", array(Range(0,salt).toList.map(lit): _*))
    val tableDf = tmpDf.withColumn("salt_ratio_s", explode(tmpDf("salt_range"))).drop("salt_range")
    val streamDf = df.withColumn("salt_ratio", monotonically_increasing_id % salt)
    val saltedExpr = streamDf("salt_ratio") === tableDf("salt_ratio_s") && joinExpression

    streamDf.join(tableDf, saltedExpr, joinType).drop("salt_ratio_s").drop("salt_ratio")
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .master("local[*]").config("spark.executor.memory", "500mb")
      .appName("Exercise1")
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

    //products_ds.show(5)
    //sellers_ds.show(5)
    //sales_ds.show(5)

    //println("Number of Products: $d".format(products_ds.count()))
    //println("Number of Sellers: $d".format(sellers_ds.count()))
    //println("Number of Sales: $d".format(sales_ds.count()))
/*    sales_ds.agg(countDistinct("product_id")
      .alias("Number_of_products_sold_at_least_once")).show()

    sales_ds.groupBy("product_id").agg(count(lit(1)).alias("cnt"))
      .sort("cnt").orderBy(desc("cnt")).show()


    sales_ds.select("date", "product_id")
      .groupBy("date").agg(countDistinct("product_id").alias("distinct_prod_sold")).show()

    val avr_revenue = sales_ds.select("date","product_id","num_pieces_sold")
      .join(products_ds.select("product_id", "price"), "product_id")
      .withColumn("price", col("price").cast(IntegerType))
      .withColumn("num_pieces_sold", col("num_pieces_sold").cast(IntegerType))
      .groupBy("date").agg(avg($"price" * $"num_pieces_sold").alias("avr_rev"))
      .orderBy(desc("date")).show(5)
*/
    val avr_revenue_2 = saltedJoin(sales_ds.select("date", "product_id", "num_pieces_sold"),
      products_ds.select("product_id", "price"), sales_ds("product_id") === products_ds("product_id"),
      "left", 5).withColumn("price", col("price").cast(IntegerType))
      .withColumn("num_pieces_sold", col("num_pieces_sold").cast(IntegerType))
      .groupBy("date").agg(avg($"price" * $"num_pieces_sold").alias("avr_rev"))
      .orderBy(desc("date")).show(5)

  }
}