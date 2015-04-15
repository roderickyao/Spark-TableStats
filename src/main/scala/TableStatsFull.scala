import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.util.{CollectionsUtils, Utils}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.PriorityQueue

object TableStatsFull {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("FooExample").setMaster("local[2]")
    sparkConf.set("spark.broadcast.compress", "false")
    sparkConf.set("spark.shuffle.spill.compress", "false")
    sparkConf.set("spark.shuffle.compress", "false")

    val sc = new SparkContext(sparkConf)

    //[(Array[Byte])]
    val rdd = sc.parallelize(Array(
      "1,1,1,0",
      "2,2,2,2",
      "1,2,3,4"))

    //Flat map which will give us (<column>|<value>, null)
    val columnIndexedRdd = rdd.flatMap( r => {
      val cells = r.split(',')

      var index = 0;

      cells.map( c => {

        val result = (index + "|" + c, null)
        index += 1
        result
      })
    })

    //Queue to hold the top 100 items
    val topItems=new mutable.Queue[String]()

    //print output for us
    //columnIndexedRdd.take(100).foreach(r => println(r))

    //will change this later when we add in multiple partitions per column
    val partitionedRDD = columnIndexedRdd.repartitionAndSortWithinPartitions(new CustomPartitioner(4))
    //This is creating our acc object and setting initial value
    //AccumulatorResults is the initial value
    //This is the logic that will hold and add up all the parts and results
    val accumulator = sc.accumulable(new AccumulatorResults, "foobar")(new CustomAccumulatorParam)

    partitionedRDD.foreachPartition(it => {
      var countMap:Map[String, Int]=Map()
      var lastValue:String = null
      var rowCount = 1
      var column:String = null
      var counter:Long=0
      var unique: Long = 0
      var nulls: Long = 0
      var emptyCells: Long = 0
      var min: Long = Long.MaxValue
      var max: Long = Long.MinValue

      it.foreach( f = r => {
        val firstPipe = r._1.indexOf('|')
        column = r._1.substring(0, firstPipe)
        val value = r._1.substring(firstPipe + 1)

        if (value == null || value==None || value.equalsIgnoreCase("NULL")) {
          nulls += 1

        }
        if (value.isEmpty)
          emptyCells += 1


        if (value.equals(lastValue)) {
          rowCount += 1
        } else {
          //countMap += (value -> true)
          if (lastValue != null) {
            //println("Y:" + column + "," + lastValue.toLong + "," + rowCount )
            val accPart = new AccumulatorRecordPart(column, lastValue.toLong, rowCount)
            accumulator.+=(accPart)
          } else {
            rowCount += 1
          }

          //Maintain the queue of top 100 items
          if(topItems.size<100){
            topItems.enqueue(r._1)
          }
          //Remove the lowest item when the queue reach its limit. Since the data is sorted, we are good here
          if(topItems.size==100){
            topItems.dequeue()
            topItems.enqueue(r._1)
          }
          unique += 1
          lastValue = value
          rowCount = 1
        }

        counter += 1

        if (value.toLong < min)
          min = value.toLong
        if (value.toLong > max)
          max = value.toLong
      })

      for(i<-0 until topItems.size){
        println("Column: " +column+" Top "+i+": "+ topItems.dequeue())
      }
      println("Column: " + column + " Unique Values:" + unique )
      println("Column: "+ column+ "  Carnality: " + unique.toDouble / counter.toDouble+ " Min: " + min + " Max: " + max)
      println("Column: " + column +"  Nulls:" + nulls +" Empty Cells: "+ emptyCells)

      if (lastValue != null) {
        //println("X:" + column + "," + lastValue.toLong + "," + rowCount )
        val accPart = new AccumulatorRecordPart(column, lastValue.toLong, rowCount)
        accumulator.+=(accPart)
      }
    })


    val accLocalValue = accumulator.localValue
    var avgMap:Map[String, Double] = Map()

    accLocalValue.columnStatsMap.foreach( r => {
      println("Column:" + r._1 + ",(Sum:" + r._2.sum + ",Count:" + r._2.count + ", Avg:" + (r._2.sum/r._2.count.toDouble) + "))")
      avgMap += (r._1 -> (r._2.sum/r._2.count.toDouble))
    })

    //why is this wrong
    //if small not a big deal
    /*
    partitionedRDD.foreach( r => {
      val firstPipe = r._1.indexOf('|')
      val column= r._1.substring(0, firstPipe)
      println(r + " avg:" + avgMap.get(column).get)
    })
    */

    val bcAvgMap = sc.broadcast[Map[String, Double]](avgMap)

    //Calculate Stadard Deviation
    partitionedRDD.foreachPartition(it=>{
      var tempsum:Double=0
      var column:String=null
      var avg:Double=0

      it.foreach(r => {
        val firstPipe = r._1.indexOf('|')
        column= r._1.substring(0, firstPipe)
        val value=r._1.substring(firstPipe+1,r._1.length())
        avg=bcAvgMap.value.get(column).get

        tempsum += Math.pow(value.toDouble-avg,2)
      })

      val count=accLocalValue.columnStatsMap.get(column).get.count
      println("Column: "+column+" Count: "+count+" AVG:"+avg+" STDDEV: "+Math.sqrt(tempsum/count))
    })

  }

  class CustomPartitioner[V](partitions: Int) extends Partitioner {

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val k = key.asInstanceOf[String]
      //This gets the index of the column and makes it the index for the partition
      k.substring(0,k.indexOf('|')).toInt
    }
  }

  class AccumulatorRecordPart(val columnName:String, val value:Long, val count:Long)

  class AccumulatorResults() extends Serializable {

    //Map (Column, ColumnStats)
    var columnStatsMap:Map[String, ColumnStats] = Map()

    def +=(that: AccumulatorRecordPart) = {

      val columnStatsOptional = columnStatsMap.get(that.columnName)
      var columnStats:ColumnStats = null
      if (columnStatsOptional.isEmpty) {
        columnStats = new ColumnStats
        columnStatsMap += (that.columnName -> columnStats)
      } else {
        columnStats = columnStatsOptional.get
      }

      //println("Adding Part: Column: " + that.columnName + " " + columnStats + " " + that.value + " " +  that.count)

      //println("Column before: " + that.columnName + " " + columnStats + " " + that.value + " " +  that.count)
      columnStats+=that
      //println("Column After: " + that.columnName + " " + columnStats)
    }

    def +=(that: AccumulatorResults) = {
      //This logic will change when we move to multi partition per column
      //but for now we only have one partition per column

      //println("Combining Partitions to Driver: Column: ")
      columnStatsMap.foreach( r => {
        //println(" - Left Map: Column:" + r._1 + ",(Sum:" + r._2.sum + ",Count:" + r._2.count + "))")
      })
      that.columnStatsMap.foreach( r => {
        //println(" - Right Map: Column:" + r._1 + ",(Sum:" + r._2.sum + ",Count:" + r._2.count + "))")
      })
      columnStatsMap ++= that.columnStatsMap
    }

  }

  class ColumnStats() extends Serializable {
    var sum:Long = 0
    var count:Long = 0

    def +=(that: AccumulatorRecordPart) = {
      this.sum += that.value * that.count
      this.count += that.count
    }

    override def toString():String = {
      "sum:" + sum + ",count:" + count
    }
  }

  class CustomAccumulatorParam extends AccumulableParam[AccumulatorResults, AccumulatorRecordPart] {
    override def zero(initialValue: AccumulatorResults): AccumulatorResults = {
      initialValue
    }

    override def addInPlace(v1: AccumulatorResults, v2: AccumulatorResults): AccumulatorResults = {
      v1 += v2
      v1
    }

    override def addAccumulator(results:AccumulatorResults, part:AccumulatorRecordPart ): AccumulatorResults = {
      results += part
      results
    }
  }

}