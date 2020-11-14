import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object FriendsDataDataset {

  case class FriendsData(id: Int, name: String, age: Int, friends: Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("FriendsDataset")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val ds = spark
      .read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/fakefriends.csv")
      .as[FriendsData]

/*    val totalsByName = ds.select("name")
      .groupBy("name").count()
      .sort("count").orderBy(asc("count")).show()*/

    val avrFriendByAge = ds.select("age","friends")
      .groupBy("age").agg(round(avg("friends"),2).alias("friends_avg"))
      .sort("age").orderBy(asc("age"))
    //avrFriendByAge.show()
    avrFriendByAge.where("age = 28").show()

  }
}
