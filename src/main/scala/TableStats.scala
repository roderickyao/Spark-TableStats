import java.lang

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.util.{CollectionsUtils, Utils}
import org.apache.spark.SparkContext.LongAccumulatorParam

object TableStats{
  def main (args: Array[String]):Unit={
    val conf=new SparkConf().setMaster("local").setAppName("TableStats")
    conf.set("spark.broadcast.compress", "false")
    conf.set("spark.shuffle.spill.compress", "false")
    conf.set("spark.shuffle.compress", "false")
    val sc=new SparkContext(conf)

    //[(Array[Byte])]
    val rdd = sc.parallelize(Array(
      "1,1,1",
      "2,2,2",
      "1,2,3"))

    val columnIndexedRdd = rdd.flatMap( r => {
      val cells = r.split(',')
      var index = 0;
      cells.map( c => {
        val result = (index + "|" + c, null)
        index += 1
        result
      })
    })

    columnIndexedRdd.take(100).foreach(r => println(r))

    //will change this later when we add in multiple partitions per column
    val partitioner: Partitioner = new customPartitioner(3);

    val partitionedRDD = columnIndexedRdd.repartitionAndSortWithinPartitions(partitioner)

    var avgList:Accumulator[Long]=sc.accumulator[Long](0)

    val partitionedRDD2 = partitionedRDD.foreachPartition( f = it => {
      var counter = 0;
      val random = Math.random();

      var sum: Long = 0
      var count: Long = 0
      var unique: Long = 0
      var nulls: Long = 0
      var emptyCells: Long = 0
      var min: Long = Long.MaxValue
      var max: Long = Long.MinValue

      var lastValue: String = null

      it.foreach(r => {
        println(random + "  " + counter + "|" + r);

        //There ris a difference from cell count and unique cell values

        val value = r._1.substring(r._1.indexOf("|") + 1)

        if (value == null || value.equalsIgnoreCase("NULL")) {
          nulls += 1

        }
        if (value.equalsIgnoreCase("") || value.equalsIgnoreCase(" "))
          emptyCells += 1

        sum += value.toLong
        count += 1

        if (value.equals(lastValue)) {
          //nothing
        } else {
          unique += 1
          lastValue = value
        }

        if (value.toLong < min)
          min = value.toLong
        if (value.toLong > max)
          max = value.toLong

        counter += 1
      })

      val average = Double.box(sum) / Double.box(count)
      avgList.add(5)

      println("Finished Partition " + random)
      println("SUM: " + sum)
      println("Average: " + average)
      //println("Std dev: "+ tmpsum/average)
      println("Carnality: " + Double.box(unique) / Double.box(count))
      println("Min: " + min)
      println("Max: " + max)
    })
println(avgList)

  }

  class customPartitioner[V](partitions: Int) extends Partitioner {

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[String]
      //This gets the index of the column and makes it the index for the partition
      k.substring(0, k.indexOf('|')).toInt
    }
  }

}