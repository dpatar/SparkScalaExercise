import org.apache.spark._
import org.apache.log4j._


object FriendsDataRDD {

  def ParseData(line: String): (String,Int,Int) = {
    val fields = line.split(",")
    val name = fields(1)
    val age = fields(2).toInt
    val numFriends = fields(3).toInt
    (name, age, numFriends)
  }

  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "Friends")
    val lines = sc.textFile("data/fakefriends-noheader.csv")
    val rdd = lines.map(ParseData)

    val totalsByName = rdd.map(x => (x._1, 1)).reduceByKey((x,y) => x+y)
      .sortBy(x=>{x._2}, false).collect()

    //totalsByName.foreach(x => println("name: %s, \n people with same name: %d".format(x._1,x._2)))

    val avrFriendsByAge = rdd.map(x => (x._2,(x._3, 1))).reduceByKey((x,y) => (x._1+y._1, x._2+y._2))
      .mapValues(x => (x._1.toFloat / x._2)).sortByKey(false).collect()

    //avrFriendsByAge.foreach(x => println("Age: %d, AvrFriends: %.2f".format(x._1, x._2)))

    val targetAge = avrFriendsByAge.filter(x => x._1==29)

    targetAge.foreach(x => println("Age: %d, AvrFriends: %.2f".format(x._1, x._2)))


  }
}
