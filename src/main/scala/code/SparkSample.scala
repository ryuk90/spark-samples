package code

import org.apache.spark.{SparkConf, SparkContext}

object SparkSample {

  def main (args: Array[String]) : Unit = {

    val conf = new SparkConf().setAppName("untitled").setMaster("local")
    val sc = new SparkContext(conf)

    // example - parallelize
    val data = Array(1,2,3,4,5)
    val distData = sc.parallelize(data)


    // example - map reduce
    val distFile = sc.textFile("/Users/ryuk/Documents/untitled/src/main/scala/code/file.txt")
    val lineLenghts = distFile.map(s => s.length)
    val totalLength = lineLenghts.reduce((a,b) => a + b)
    println("totalLength: ", totalLength)


    // example - persist - if "lineLenghts" is needed later, is possible to keep it on the cluster for more time with
    lineLenghts.persist()


    
  }

}