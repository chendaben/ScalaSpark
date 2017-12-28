/**
  * Created by aseara on 2017/6/1.
  */

package com.chinacloud.metagrid

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import scala.collection.mutable.ArrayBuffer






/**
  * Created by qiujingde on 2017/5/31.
  * spark 集群测试1：随机点计算圆周率
  */
object DataCheck {
  def main(args: Array[String]) {

    var reg = args(0)
    var urlSrc = args(1)
    var taskname = args(2)
    var table = args(3)
    var filed = args(4)
    var userSrc = args(5)
    var passedSrc= args(6)

    var urlDest = args(7)
    var tableDest = args(8)
    var userDest = args(9)
    var passedDest= args(10)

    val sparksession = SparkSession
      .builder
      .appName(taskname)
      .getOrCreate()

    //        var reg = "\\d{11}"
    //        var urlSrc = "jdbc:mysql://172.16.50.21:3306/guan_data"
    //        var taskname = "SparkJdbc"
    //        var table = "record"
    //        var filed = "execption_key"
    //        var userSrc = "metagrid"
    //        var passedSrc="metagrid"
    //
    //        var urlDest = "jdbc:mysql://172.16.50.21:3306/guan_data"
    //        var tableDest = "guan_data.exampledata"
    //        var userDest = "metagrid"
    //        var passedDest="metagrid"



    //获取JobId
    var jobId = sparksession.sparkContext.applicationId

    val dataFrame = readMysqlTable(sparksession,urlSrc,table,userSrc,passedSrc)

    val srcdata = dataFrame.select(filed).rdd.map(f=>{
      f.get(0).toString
    })

    var resultmap =  regualition(reg,srcdata.collect().toBuffer.toList,filed)
    resultmap += ("jobId"->jobId)
    resultmap += ("taskname"->taskname)
//    println(resultmap.toString())

    var task_name = resultmap.get("taskname")
    var job_id = resultmap.get("jobId")
    var check_time = resultmap.get("checktime")
    var validcount = resultmap.get("valid").map(f=>{
      f.toInt
    })
    var nullcount = resultmap.get("nulll").map(f=>{
      f.toInt
    })
    var invalidcount = resultmap.get("invalid").map(f=>{
      f.toInt
    })
    var invalidExample = resultmap.get("invalidexample")

    var validpercent = validcount.getOrElse(0)*10000/srcdata.collect().toBuffer.toList.length
    var invalidpercent = invalidcount.getOrElse(0)*10000/srcdata.collect().toBuffer.toList.length
    var nullpercent = nullcount.getOrElse(0)*10000/srcdata.collect().toBuffer.toList.length

    //获取时间
    var now:Date = new Date()
    var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
    var checktime = dateFormat.format( now )

    var valStr = (task_name.getOrElse("dataquality"),job_id.getOrElse("job_000"),1,"合格",validcount.getOrElse(0),validpercent,"",checktime)
    var invalstr = (task_name.getOrElse("dataquality"),job_id.getOrElse("job_000"),0,"不合格",invalidcount.getOrElse(0),invalidpercent,resultmap.get("invalidexample").getOrElse(" ").toString,checktime)
    var nullStr = (task_name.getOrElse("dataquality"),job_id.getOrElse("job_000"),0,"空",nullcount.getOrElse(0),nullpercent,"",checktime)

    val sc = sparksession.sparkContext
    val resultRdd = sc.parallelize(Array(valStr,invalstr,nullStr))

    val schema = StructType(
      List(
        StructField("task_name", StringType, false),
        StructField("job_id", StringType, false),
        StructField("is_valid",IntegerType, false),
        StructField("check_type", StringType, false),
        StructField("count", IntegerType, false),
        StructField("percent", IntegerType, false),
        StructField("example_value", StringType, false),
        StructField("check_time",StringType,false)
      )
    )

    val rowRDD = resultRdd.map(p => Row(p._1,p._2,p._3,p._4,p._5,p._6,p._7,p._8))
    val personDataFrame = sparksession.sqlContext.createDataFrame(rowRDD, schema)

    val prop = new Properties()
    prop.put("user",userDest )
    prop.put("password", passedDest)

    personDataFrame.write.mode("append").jdbc(urlDest,tableDest,prop)

    sparksession.stop()

  }


  def writeMysqlTTables(iterator: Iterator[(String, String,Boolean,String,Int,Int,String,String)]): Unit =
  {

    var url = "jdbc:mysql://172.16.50.21:3306/guan_data"
    var user = "metagrid"
    var password = "metagrid"

    println(url)
    var conn:Connection= null
    var ps:java.sql.PreparedStatement=null

    val sql = "insert into exampledata(task_name,job_id,is_valid,check_type,count,percent,example_value,check_time) values (?,?,?,?,?,?,?,?)"

    conn= DriverManager.getConnection(url,user,password)

    ps = conn.prepareStatement(sql)
    iterator.foreach(data=>{
      ps.setString(1, data._1)
      ps.setString(2, data._2)
      ps.setBoolean(3,data._3)
      ps.setString(4,data._4)
      ps.setInt(5,data._5)
      ps.setInt(6,data._6)
      ps.setString(7,data._7)
      ps.setString(8, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Timestamp(System.currentTimeMillis)))
      ps.executeUpdate()
    })

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




  def regualition(regx:String,dataList:List[String],filed:String): Map[String,String]=
  {
    var invalidexample: collection.mutable.ListBuffer[String] = collection.mutable.ListBuffer[String]()

    val totalCount = dataList.length
    var invalid = 0
    var valid = 0
    var nulll = 0
    var result:Map[String,String] = Map()
    var exampledata:Map[String,String] = Map()
    val ab = collection.mutable.ArrayBuffer[String]()


    for(data <- dataList)
    {
      if(data == null || data.equals(" ")||data.equals(""))
      {
        nulll += 1
      }
      else
      {
        val isvlid = data.matches(regx)

        if(isvlid == true)
        {
          valid += 1
        }
        else
        {
          invalid +=1

          if(invalidexample.length < 5)
          {
            invalidexample += data
            ab += data

          }

        }
      }
    }

    exampledata += ("不合格数据采样"->ab.mkString(",").toString)

    var dd = Json(DefaultFormats).write(exampledata)


    result += ("totalCount" -> totalCount.toString)
    result += ("nulll" -> nulll.toString)
    result += ("invalid" -> invalid.toString)
    result += ("valid" -> valid.toString)
    result += ("invalidexample" -> dd)

    return result
  }
}
