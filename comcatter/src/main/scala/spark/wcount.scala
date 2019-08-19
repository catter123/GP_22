package spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author catter
  * @date 2019/7/29 20:18
  */
object wcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wcount").setMaster("local[2]")
    val context = new SparkContext(conf)
    val file = context.textFile("hdfs://hadoop02/aaa")
    val endd = file.flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_).sortBy(_._2,false)
    endd.saveAsTextFile("C:\\Users\\catter\\Desktop\\bbb.txt")
    context.stop()



  }

}
