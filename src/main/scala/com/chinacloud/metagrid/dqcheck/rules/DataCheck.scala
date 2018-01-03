/**
  * Created by aseara on 2017/6/1.
  */

package com.chinacloud.metagrid.dqcheck.rules

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json



/**
  * Created by qiujingde on 2017/5/31.
  * spark 集群测试1：随机点计算圆周率
  */
object DataCheck {
  def main(args: Array[String]) {

    //宏观数据和不符合数据样本
      var reg = args(0)
      var taskName = args(1)
      var table = args(2)
      var filed = args(3)
      var urlDest = args(4)
      var statisticsTable = args(5)
      var detailsTable = args(6)
      var userDest = args(7)
      var passwdDest= args(8)
      println("输入正则表达式为：" + reg)
      println("任务名称为：" + taskName)
      println("查询的表名为：" + table)
      println("检测的字段为：" + filed)
      println("结果数据库URL为：" + urlDest)
      println("宏观统计结果输出表为：" + statisticsTable)
      println("不合格数据录入表为：" + detailsTable)
      println("结果数据库用户名为：" + userDest)
      println("结果数据库密码为：" + passwdDest)

//一下参数只针对于宏观数据统计
//    var reg = args(0)
//    var taskName = args(1)
//    var table = args(2)
//    var filed = args(3)
//    var urlDest = args(4)
//    var statisticsTable = args(5)
//    var userDest = args(6)
//    var passwdDest= args(7)

    val sparksession = SparkSession
      .builder
      .appName(taskName)
      .enableHiveSupport()
      .getOrCreate()
    println("--------字段合法性检测开始")
    //获取JobId
    var jobId = sparksession.sparkContext.applicationId
    var sql:String = "select * from" +" "+ table

    //读取Mysql数据库
//    val dataFrame = readMysqlTable(sparksession,urlSrc,table,userSrc,passwdSrc)
//    var srcdata = dataFrame.select("*")

    //读取hive数据库
    println("--------开始读取数据表信息")
    val srcdata = sparksession.sql(sql)
    println("--------读取数据表信息完成")
    var resultMap = regualateForMacro(reg,srcdata,filed);
    resultMap +=("taskName"->taskName)
    resultMap +=("jobId"->jobId)

    writeMacroResultToMysql(sparksession,resultMap,urlDest,statisticsTable,userDest,passwdDest)
    println("--------宏观统计数据写入数据库完成")

    var dataList:collection.mutable.ListBuffer[String] = regualateForDetail(reg,srcdata,filed)
    writedetailResultToMysql(sparksession,taskName,jobId,dataList,urlDest,detailsTable,userDest,passwdDest)
    println("--------不合格信息写入数据库完成")

    println("--------字段合法性检测完成")
    sparksession.stop()

  }

  /**
    * 将不合格数据写入数据表
    * @param session
    * @param taskName
    * @param jobId
    * @param resultDetails
    * @param urlDestDB
    * @param detailTable
    * @param user
    * @param passwd
    */
  def writedetailResultToMysql(session:SparkSession,taskName:String,jobId:String ,resultDetails:collection.mutable.ListBuffer[String], urlDestDB: String, detailTable: String, user: String, passwd: String): Unit =
  {

    var invalidexample: collection.mutable.ListBuffer[(String,String,String)] = collection.mutable.ListBuffer[(String,String,String)]()

    var vtaskName = taskName
    var vjobId = jobId
    var dataList = resultDetails

    for(element <- dataList)
      {
        var dataStr =(vtaskName.toString,vjobId.toString,element.toString)
        invalidexample += dataStr
      }

    val sc = session.sparkContext
    val resultRdd = sc.parallelize(invalidexample)


    val schema = StructType(
      List(
        StructField("task_name", StringType, false),
        StructField("job_id", StringType, false),
        StructField("example_data", StringType, false)
      )
    )

    val rowRDD = resultRdd.map(p => Row(p._1,p._2,p._3))
    val resultDataFrame = session.sqlContext.createDataFrame(rowRDD, schema)

    val prop = new Properties()
    prop.put("user",user )
    prop.put("password", passwd)
    resultDataFrame.write.mode("append").jdbc(urlDestDB,detailTable,prop)

  }

  /**
    * 讲宏观结果数据写入数据表
    * @param sparkSession
    * @param resultMap
    * @param urlDestDB
    * @param statisticsTable
    * @param user
    * @param passwd
    */
  def writeMacroResultToMysql(sparkSession:SparkSession,resultMap:Map[String,Any],urlDestDB:String,statisticsTable:String,user:String,passwd:String): Unit =
  {
    var dataLength = resultMap.get("totalCount").map(f=>{
      f.toString.toInt
    }).getOrElse(8888)
    var taskName = resultMap.get("taskName")
    var jobId = resultMap.get("jobId")
    var validCount = resultMap.get("validCount").map(f=>{
      f.toString.toInt
    })
    var nullCount = resultMap.get("nullCount").map(f=>{
      f.toString.toInt
    })
    var invalidCount = resultMap.get("inValidCount").map(f=>{
      f.toString.toInt
    })

    var validPercent = validCount.getOrElse(0)*10000/dataLength
    var invalidPercent = invalidCount.getOrElse(0)*10000/dataLength
    var nullPercent = nullCount.getOrElse(0)*10000/dataLength

    var ts = new Timestamp(System.currentTimeMillis())

    var valStr = (taskName.getOrElse("dataquality"),jobId.getOrElse("job_000"),1,"合格",validCount.getOrElse(0),validPercent,ts)
    var invalstr = (taskName.getOrElse("dataquality"),jobId.getOrElse("job_000"),0,"不合格",invalidCount.getOrElse(0),invalidPercent,ts)
    var nullStr = (taskName.getOrElse("dataquality"),jobId.getOrElse("job_000"),0,"空",nullCount.getOrElse(0),nullPercent,ts)

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
        StructField("check_time",TimestampType,false)
      )
    )

    val rowRDD = resultRdd.map(p => Row(p._1,p._2,p._3,p._4,p._5,p._6,p._7))
    val resultDataFrame = sparkSession.sqlContext.createDataFrame(rowRDD, schema)

    val prop = new Properties()
    prop.put("user",user )
    prop.put("password", passwd)
    resultDataFrame.write.mode("append").jdbc(urlDestDB,statisticsTable,prop)

  }

  /**
    * 获取不合格数据详情
    * @param regx
    * @param dataFrame
    * @param filed
    * @return
    */
  def regualateForDetail(regx:String,dataFrame: DataFrame,filed:String): collection.mutable.ListBuffer[String] =
  {
    println("--------开始录入不合格数据")
    var invalidexample: collection.mutable.ListBuffer[String] = collection.mutable.ListBuffer[String]()
    var totalCount = dataFrame.count()
    var columsList = dataFrame.columns.toList
    var feildIndex = columsList.indexOf(filed)
    var columsArray = columsList.toArray

    dataFrame.collect().map(f=>{
      var data = f.get(feildIndex).toString

        val isvlid = data.matches(regx)

        if(isvlid == false)
        {

          if(invalidexample.length < 100)
          {
            var dLength = f.length
            var picResult:Map[String,String] = Map()
            for(a <- 0 to dLength-1)
            {
              picResult += (columsArray(a) -> f.get(a).toString)
            }

            var dd = Json(DefaultFormats).write(picResult)

            invalidexample += dd
          }
        }
    })
    println("--------不合格数据录入完成")
    return invalidexample
  }

  /**
    * 统计宏观数据
    * @param regx
    * @param dataFrame
    * @param filed
    * @return
    */
  def regualateForMacro(regx:String,dataFrame: DataFrame,filed:String):Map[String,Any] =
  {
    println("--------开始宏观数据统计")
    var invalidexample: collection.mutable.ListBuffer[String] = collection.mutable.ListBuffer[String]()
    var invalidCount = 0
    var validCount = 0
    var nullCount = 0
    var result:Map[String,Any] = Map()


    var totalCount = dataFrame.count()
    var columsList = dataFrame.columns.toList
    var feildIndex = columsList.indexOf(filed)
    var columsArray = columsList.toArray

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

        }
      }
    })
    result += ("totalCount" -> totalCount.toString)
    result += ("nullCount" -> nullCount.toString)
    result += ("inValidCount" -> invalidCount.toString)
    result += ("validCount" -> validCount.toString)
    println("--------宏观数据统计完成")
    return result;
  }

  /**
    * 读取Mysql数据库，返回dataframe
    * @param ss
    * @param url
    * @param dbtable
    * @param user
    * @param password
    * @return
    */
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
