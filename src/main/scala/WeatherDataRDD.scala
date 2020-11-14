import org.apache.spark._
import org.apache.log4j._

import scala.math.min

object WeatherDataRDD {

  def parseLine(line:String): (String, String, Float)={
    val field = line.split(",")
    val StationID = field(0)
    val entryType = field(2)
    val temperature = field(3).toFloat
    (StationID, entryType, temperature)
  }

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "WeatherData")
    val lines = sc.textFile("data/1800.csv")
    val rdd = lines.map(parseLine)

    val minTempByStation = rdd.filter(x => x._2=="TMIN").map(x => (x._1,x._3))
      .reduceByKey((x,y) => (min(x,y))).sortBy(x => x._2, true).collect()

    minTempByStation.foreach(println)
  }
}
