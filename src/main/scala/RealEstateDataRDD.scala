import org.apache.spark._
import org.apache.log4j._

object RealEstateDataRDD {

  def parseLine(line: String): (Float, Float, Float, Int, Float) = {
    val field = line.split(",")
    val date = field(1).toFloat
    val houseAge = field(2).toFloat
    val distToMRT = field(3).toFloat
    val numConStores = field(4).toInt
    val pricePerUnit = field(7).toFloat
    (date, houseAge, distToMRT, numConStores, pricePerUnit)
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "EstateData")
    val lines = sc.textFile("data/realestate.csv").filter(x => !x.startsWith("No"))
    val rdd = lines.map(parseLine).cache()

    rdd.take(10).foreach(println)
  }
}
