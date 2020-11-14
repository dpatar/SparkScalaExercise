import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StringType, IntegerType, FloatType}
import org.apache.log4j._

object WeatherDataDataset {

  case class temperature(stationId: String, date: Int, measure_type: String, temp: Float)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("WeatherDataset")
      .master("local[*]")
      .getOrCreate()

    val temperatureSchema = new StructType()
      .add("stationId", StringType, nullable = true)
      .add("date", IntegerType, nullable = true)
      .add("measure_type", StringType, nullable = true)
      .add("temp", FloatType, nullable = true)

    import spark.implicits._
    val ds = spark
      .read
      .option("header", false)
      .schema(temperatureSchema)
      .csv("data/1800.csv")
      .as[temperature]

    val minTempByStation = ds.where($"measure_type" === "TMIN")
      .select("stationId","temp")
      .groupBy("stationId").min("temp")

    val results = minTempByStation.collect()

    for (result <- results){
      val station = result(0)
      val temp = result(1).asInstanceOf[Float]
      val formattedTemp = f"$temp%.2f F"

      println(s"$station minimum temperature: $formattedTemp")
    }

  }
}
