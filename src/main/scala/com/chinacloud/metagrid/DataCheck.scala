/**
  * Created by aseara on 2017/6/1.
  */

package com.chinacloud.metagrid


import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}



/**
  * Created by qiujingde on 2017/5/31.
  * spark 集群测试1：随机点计算圆周率
  */
object DataCheck {
  def main(args: Array[String]) {

    var reg = args(0)
    var urlSrc = args(1)
    var taskName = args(2)
    var table = args(3)
    var filed = args(4)
    var userSrc = args(5)
    var passwdSrc= args(6)

    var urlDest = args(7)
    var statisticsTable = args(8)
    var detailsTable = args(9)
    var userDest = args(10)
    var passwdDest= args(11)

    val sparksession = SparkSession
      .builder
      .appName(taskName)
      .master("local[1]")
      .getOrCreate()

    //获取JobId
    var jobId = sparksession.sparkContext.applicationId

    val dataFrame = readMysqlTable(sparksession,urlSrc,table,userSrc,passwdSrc)

    val srcdata = dataFrame.select("*")

    var resultMap = regualate(reg,srcdata,filed);
    resultMap +=("taskName"->taskName)
    resultMap +=("jobId"->jobId)
    writeResultToMysql(sparksession,resultMap,urlDest,statisticsTable,detailsTable,userDest,passwdDest)

    sparksession.stop()

  }

  def writeResultToMysql(sparkSession:SparkSession,resultMap:Map[String,String],urlDestDB:String,statisticsTable:String,detailsTable:String,user:String,passwd:String): Unit =
  {
    var dataLength = resultMap.get("totalCount").map(f=>{
      f.toInt
    }).getOrElse(8888)
    var taskName = resultMap.get("taskName")
    var jobId = resultMap.get("jobId")
    var validCount = resultMap.get("validCount").map(f=>{
      f.toInt
    })
    var nullCount = resultMap.get("nullCount").map(f=>{
      f.toInt
    })
    var invalidCount = resultMap.get("inValidCount").map(f=>{
      f.toInt
    })

    var validPercent = validCount.getOrElse(0)*10000/dataLength
    var invalidPercent = invalidCount.getOrElse(0)*10000/dataLength
    var nullPercent = nullCount.getOrElse(0)*10000/dataLength


    var now:Date = new Date()
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
    var checktime = dateFormat.format( now )

    var valStr = (taskName.getOrElse("dataquality"),jobId.getOrElse("job_000"),1,"合格",validCount.getOrElse(0),validPercent,checktime)
    var invalstr = (taskName.getOrElse("dataquality"),jobId.getOrElse("job_000"),0,"不合格",invalidCount.getOrElse(0),invalidPercent,checktime)
    var nullStr = (taskName.getOrElse("dataquality"),jobId.getOrElse("job_000"),0,"空",nullCount.getOrElse(0),nullPercent,checktime)

    val sc = sparkSession.sparkContext
    val resultRdd = sc.parallelize(Array(valStr,invalstr,nullStr))

    val schema = StructType(
      List(
        StructField("task_name", StringType, false),
        StructField("job_id", StringType, false),
        StructField("is_valid",IntegerType, false),
        StructField("check_type", StringType, false),
        StructField("count", IntegerType, false),
        StructField("percent", IntegerType, false),
        StructField("check_time",StringType,false)
      )
    )

    val rowRDD = resultRdd.map(p => Row(p._1,p._2,p._3,p._4,p._5,p._6,p._7))
    val personDataFrame = sparkSession.sqlContext.createDataFrame(rowRDD, schema)

    val prop = new Properties()
    prop.put("user",user )
    prop.put("password", passwd)
    personDataFrame.write.mode("append").jdbc(urlDestDB,statisticsTable,prop)

  }

  def regualate(regx:String,dataFrame: DataFrame,filed:String):Map[String,String] =
  {
    var invalidexample: collection.mutable.ListBuffer[String] = collection.mutable.ListBuffer[String]()
    var invalidCount = 0
    var validCount = 0
    var nullCount = 0
    var result:Map[String,String] = Map()


    var totalCount = dataFrame.count()
    var columsArray = dataFrame.columns.toList
    var feildIndex = columsArray.indexOf(filed)

    dataFrame.collect().map(f=>{
      var data = f.get(feildIndex).toString
      if(data == null || data.equals(" ")||data.equals(""))
      {
        nullCount += 1
      }
      else
      {
        val isvlid = data.matches(regx)

        if(isvlid == true)
        {
          validCount += 1
        }
        else
        {
          invalidCount +=1

          if(invalidexample.length < 100)
          {
            invalidexample += f.toString()
          }
        }
      }
    })

    result += ("totalCount" -> totalCount.toString)
    result += ("nullCount" -> nullCount.toString)
    result += ("inValidCount" -> invalidCount.toString)
    result += ("validCount" -> validCount.toString)
    result += ("inValidExample" -> invalidexample.toString())

    return result;
  }


  def readMysqlTable(ss:SparkSession,url:String,dbtable:String,user:String,password:String): org.apache.spark.sql.DataFrame =
  {

    val driver = "com.mysql.jdbc.Driver"

    val dataFrame = ss.read.format("jdbc").options(Map("url" -> url,
      "driver" -> driver,
      "dbtable" -> dbtable,
      "user" -> user,
      "password" -> password)).load()
    println("连接数据库成功")

    return dataFrame

  }

}
