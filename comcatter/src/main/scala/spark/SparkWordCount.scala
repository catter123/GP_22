package spark

/**
  * @author catter
  * @date 2019/7/29 12:58
  */
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//spark版本的wordcount
object SparkWordCount {
  def main(args: Array[String]): Unit = {
    //ps:模板封装成一个方法以后调用方法即可
    //模板代码
    /*
     需要创建SparkConf()对象 相当于MR中配置
     必传参数
     setAppName() 设置任务的名称 不传默认是一个UUID产生名字
     设置运行模式
     不写这个参数可以打包提交集群
     写这个参数设置本地模式
     setMaster() 传入的参数有如下写法
     "local"   --> 本地一个线程来进行任务处理
     "local[数值]" --> 开始相应数值的线程来模拟spark集群运行任务
     "local[*]" --> 开始相应线程数来模拟spark集群运行任务
     两者区别:
     数值类型--> 使用当前数值个数来进行处理
     * -->当前程序有多少空闲线程就用多少空闲线程处理
     */
    val conf = new SparkConf().setAppName("SparkWordCount")
    conf.setMaster("local[2]")
    //创建sparkContext对象
    val sc =  new SparkContext(conf)
    //通过sparkcontext对象就可以处理数据

    //读取文件 参数是一个String类型的字符串 传入的是路径
    val lines: RDD[String] = sc.textFile("C:\\Users\\catter\\Desktop\\aaa.txt")

    //切分数据
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //将每一个单词生成元组 (单词,1)
    val tuples: RDD[(String, Int)] = words.map((_,1))

    //spark中提供一个算子 reduceByKey 相同key 为一组进行求和 计算value
    val sumed: RDD[(String, Int)] = tuples.reduceByKey(_+_)

    //对当前这个结果进行排序 sortBy 和scala中sotrBy是不一样的 多了一个参数
    //默认是升序  false就是降序
    val sorted: RDD[(String, Int)] = sumed.sortBy(_._2,false)

    //将数据提交到集群存储 无法返回值
    sorted.saveAsTextFile("C:\\Users\\catter\\Desktop\\bbb.txt")

    //本地模式
    //一定要设置setMaster()
    //可以直接打印
    //println(sorted.collect.toBuffer)
    //这种打印也可以
    //sorted.foreach(println)

    //回收资源停止sc,结束任务
    sc.stop()
  }
}
