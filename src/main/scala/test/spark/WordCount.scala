package test.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  final def main(args: Array[String]) {
    val sparkConf = new SparkConf
    sparkConf.setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    //val input = List("the","big","bad","wolf","jumped","over","the","lazy","fox")
    //val rdd = sc.parallelize(input)

    val rdd = sc.textFile("hdfs://localhost:9000/test/input/sample.txt")
    rdd.mapPartitions(_.flatMap(x => {
      val words = x.toLowerCase.split(" ", -1)
      words.flatMap(y => {
        if(y.isEmpty) None
        else Some(y, 1)
      })
    }))
      .reduceByKey((x, y) => x + y)
      .saveAsTextFile("hdfs://localhost:9000/test/output")
  }
}
