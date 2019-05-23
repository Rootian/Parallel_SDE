import java.util.Properties

import latest.SDE
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
//import com.databricks.spark.csv._
import org.apache.spark.sql.types._
//import com.databricks.spark.csv.CsvParser;
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.HashMap

object ParallelCompute {
  case class SDE(IndusAndYear: String, SigmaX: Double, SigmaY: Double, Theta:Double)
  def main(arg:Array[String]):Unit={
    val conf = new SparkConf().setAppName("ChongQing").setMaster("local[8]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._


    val now: Date=new Date()
    val cqDF = sqlContext.read.format("csv").option("header","true").load("/Users/aaronkb/Desktop/Projects_data/SpatiotemporalAnalysisofindustries/cqData.csv")
    cqDF.registerTempTable("cqData")
    val indusAndYearDF = sqlContext.sql("SELECT concat(F_INDUSTRY,year(F_FOUND_DATE)) as IndusAndYear, F_WGS_LNG as Lng, F_WGS_LAT as Lat FROM cqData").na.drop()
    indusAndYearDF.registerTempTable("data")
    val avgDF = sqlContext.sql("SELECT IndusAndYear, count(*) as count, avg(Lng) as mean_Lng, avg(Lat) as mean_Lat from data group by IndusAndYear")
    avgDF.cache

    val avgtemp = avgDF.map(r => (r(0).toString(),(r(1).toString().toDouble,r(2).toString().toDouble,r(3).toString().toDouble))).collect().toMap
    val avgDF_bc = sc.broadcast(avgtemp)
    val xy_dev = indusAndYearDF.map(x => {
      val avg = avgDF_bc.value
      val key = x(0).toString()
      val mean_x = avg.get(key).toList(0)._2
      val mean_y = avg.get(key).toList(0)._3
      val dev_x = x(1).toString().toDouble - mean_x
      val dev_y = x(2).toString().toDouble - mean_y

      (key,(dev_x,dev_y,mean_x,mean_y))
    }).rdd
//    xy_dev2.show()
//    val all = indusAndYearDF.join(avgDF,indusAndYearDF("IndusAndYear")===avgDF("IndusAndYear"))

//    val xy_dev=all.map { x => (x(0).toString(),(x(1).toString().toDouble-x(5).toString().toDouble,x(2).toString().toDouble-x(6).toString().toDouble,x(5).toString().toDouble,x(6).toString().toDouble ))}.rdd
//    xy_dev.cache
    val devTest = xy_dev.mapPartitions(p =>{
      var result:Map[String,Tuple4[Double,Double,Double,Double]] = Map()
      while(p.hasNext){
        val nextP = p.next()
        val temp = (nextP._1,(nextP._2._1*nextP._2._2,nextP._2._1*nextP._2._1,nextP._2._2*nextP._2._2,1.0))
        if(result.contains(temp._1)){
          //reduce by key inside partition
          val temp_before = result(temp._1)
          result -= temp._1
          val temp_after = (nextP._1,(temp._2._1 + temp_before._1,temp._2._2 + temp_before._2, temp._2._3 + temp_before._3, temp._2._4 + temp_before._4))
          result += temp_after
        }
        else {
          result += temp
        }

      }

      result.iterator
    })
    val sum = devTest.reduceByKey((x1,x2)=>(x1._1+x2._1,x1._2+x2._2,x1._3+x2._3,x1._4+x2._4),400)

    val theta = sum.mapValues(x => {
      val theta = math.atan(((x._2 - x._3) + math.sqrt((x._2 - x._3) * (x._2 - x._3) + 4 * (x._1*x._1))) / (2*x._1))
      //sigma x
      val item_x2 = math.cos(theta) * math.cos(theta) * x._2
      val item_y2 = math.sin(theta) * math.sin(theta) * x._3
      val item_xy = 2 * math.cos(theta) * math.sin(theta) * x._1
      val sigma_x = math.sqrt(2 * (item_x2 + item_y2 - item_xy)) / x._4

      //sigma y
      val item2_y2 = math.cos(theta) * math.cos(theta) * x._3
      val item2_x2 = math.sin(theta) * math.sin(theta) * x._2
      val item2_xy = 2 * math.cos(theta) * math.sin(theta) * x._1
      val sigma_y = math.sqrt(2 * (item_x2 + item_y2 + item_xy)) / x._4


      (theta,sigma_x,sigma_y)
    })

//    theta.toDF().show()
//    xy_dev.unpersist(true)
    val thetaRdd = theta.toDF()
    val result = thetaRdd.join(avgDF,avgDF("IndusAndYear") === thetaRdd("_1")).na.drop()
    result.show()
//    val a = result.rdd.foreach(p=>{while(true){}})
//    result.show()
//    result.count()


    val now2: Date=new Date()
    val now3=now2.getTime -now.getTime
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("mm:ss")
    val date = dateFormat.format(now3)   //��Ϊ��ȡ����ʱ���
    println("totalTime:"+date)
  }


}

