package com.chinacloud.metagrid.dqcheck.rules

import java.sql.Timestamp
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

/**
  * 正则表达式规则
  * Created by root on 18-1-11.
  */
object DataCheck {
    def main(args: Array[String]) {

        //传入参数
        val reg = args(0)
        val taskName = args(1)
        val table = args(2)
        val filed = args(3)
        val urlDest = args(4)
        val statisticsTable = args(5)
        val detailsTable = args(6)
        val userDest = args(7)
        val passwdDest = args(8)

        println("输入正则表达式为：" + reg)
        println("任务名称为：" + taskName)
        println("查询的表名为：" + table)
        println("检测的字段为：" + filed)
        println("结果数据库URL为：" + urlDest)
        println("宏观统计结果输出表为：" + statisticsTable)
        println("不合格数据录入表为：" + detailsTable)
        println("结果数据库用户名为：" + userDest)
        println("结果数据库密码为：" + passwdDest)

        val sparkSession = SparkSession
          .builder
          .appName(taskName)
          .enableHiveSupport()
          .getOrCreate()

        println("--------字段合法性检测开始")

        //获取JobId
        val jobId = sparkSession.sparkContext.applicationId

        //读取hive数据库
        println("--------开始读取数据表信息")
        val sql = "select * from" + " " + table
        val srcdata = sparkSession.sql(sql)
        println("--------读取数据表信息完成")


        var (resultMap: Map[String, Any], dataList: collection.mutable.ListBuffer[String]) = regulate(reg, srcdata, filed);
        resultMap += ("taskName" -> taskName)
        resultMap += ("jobId" -> jobId)


        println("--------开始宏观统计数据写入数据库")
        writeMacroResultToMysql(sparkSession, resultMap, urlDest, statisticsTable, userDest, passwdDest)
        println("--------宏观统计数据写入数据库完成")


        println("--------开始写入不合格信息到数据库")
        writedetailResultToMysql(sparkSession, taskName, jobId, dataList, urlDest, detailsTable, userDest, passwdDest)
        println("--------不合格信息写入数据库完成")

        println("--------字段合法性检测完成")
        sparkSession.stop()

    }

    /**
      * 将不合格数据样列数据写入数据库
      *
      * @param session
      * @param taskName 任务名称
      * @param jobId 任务ID
      * @param resultDetails
      * @param urlDestDB 数据库url
      * @param detailTable 存入数据的表名
      * @param user 用户名
      * @param passwd 密码
      */
    def writedetailResultToMysql(session: SparkSession, taskName: String, jobId: String, resultDetails: collection.mutable.ListBuffer[String], urlDestDB: String, detailTable: String, user: String, passwd: String): Unit = {

        var invalidexample: collection.mutable.ListBuffer[(String, String, String)] = collection.mutable.ListBuffer[(String, String, String)]()

        for (element <- resultDetails) {
            var dataStr = (taskName.toString, jobId.toString, element.toString)
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

        val rowRDD = resultRdd.map(p => Row(p._1, p._2, p._3))
        val resultDataFrame = session.sqlContext.createDataFrame(rowRDD, schema)

        val prop = new Properties()
        prop.put("user", user)
        prop.put("password", passwd)
        resultDataFrame.write.mode("append").jdbc(urlDestDB, detailTable, prop)

    }

    /**
      * 宏观结果数据写入数据库
      *
      * @param sparkSession
      * @param resultMap
      * @param urlDestDB 数据库url
      * @param statisticsTable 表名
      * @param user 用户名
      * @param passwd 密码
      */
    def writeMacroResultToMysql(sparkSession: SparkSession, resultMap: Map[String, Any], urlDestDB: String, statisticsTable: String, user: String, passwd: String): Unit = {
        val taskName = resultMap.get("taskName")
        val jobId = resultMap.get("jobId")

        val dataLength:Long = resultMap.get("totalCount").map(f => {
            f.toString.toLong
        }).getOrElse(0)

        val validCount:Long = resultMap.get("validCount").map(f => {
            f.toString.toLong
        }).getOrElse(0)

        val nullCount:Long  = resultMap.get("nullCount").map(f => {
            f.toString.toLong
        }).getOrElse(0)

        val invalidCount:Long  = resultMap.get("inValidCount").map(f => {
            f.toString.toLong
        }).getOrElse(0)


        val validPercent = (validCount * 10000 / dataLength).toInt
        val invalidPercent = (invalidCount * 10000 / dataLength).toInt
        val nullPercent = (nullCount * 10000 / dataLength).toInt

        val ts = new Timestamp(System.currentTimeMillis())

        val valStr = (taskName.getOrElse("dataquality"), jobId.getOrElse("job_000"), 1, "合格", validCount, validPercent, ts)
        val invalstr = (taskName.getOrElse("dataquality"), jobId.getOrElse("job_000"), 0, "不合格", invalidCount, invalidPercent, ts)
        val nullStr = (taskName.getOrElse("dataquality"), jobId.getOrElse("job_000"), 0, "空", nullCount, nullPercent, ts)

        val sc = sparkSession.sparkContext
        val resultRdd = sc.parallelize(Array(valStr, invalstr, nullStr))

        val schema = StructType(
            List(
                StructField("task_name", StringType, false),
                StructField("job_id", StringType, false),
                StructField("is_valid", IntegerType, false),
                StructField("check_type", StringType, false),
                StructField("count", LongType, false),
                StructField("percent", IntegerType, false),
                StructField("check_time", TimestampType, false)
            )
        )

        val rowRDD = resultRdd.map(p => Row(p._1, p._2, p._3, p._4, p._5, p._6, p._7))
        val resultDataFrame = sparkSession.sqlContext.createDataFrame(rowRDD, schema)

        val prop = new Properties()
        prop.put("user", user)
        prop.put("password", passwd)
        resultDataFrame.write.mode("append").jdbc(urlDestDB, statisticsTable, prop)

    }


  /**
    * 宏观统计计算
    * @param regx 正则表达式
    * @param dataFrame
    * @param filed 列名
    * @return
    */
    def regulate(regx: String, dataFrame: DataFrame, filed: String): (Map[String, Any], collection.mutable.ListBuffer[String]) = {
        println("--------开始统计数据计算")
        var invalidexample: collection.mutable.ListBuffer[String] = collection.mutable.ListBuffer[String]()
//        var invalidCount = 0
//        var validCount = 0
//        var nullCount = 0
        var result: Map[String, Any] = Map()
        val columsList = dataFrame.columns.toList
        var ret: List[String] = List()

        val totalCount = dataFrame.count()
        val intermediate_data: RDD[List[String]] = dataFrame.rdd.map(f => {
            val index = f.fieldIndex(filed)

            var isNull = f.isNullAt(index)
            if(isNull == true || f.get(index).equals(""))
            {
                ret = List("empty") ::: f.toSeq.toList.map(r=>{if (r != null) r.toString else ""})
            }
            else
            {
                val data = f.get(index).toString

                val isvlid = data.matches(regx)
                if (isvlid == true)
                {
                    ret = List("valid") ::: f.toSeq.toList.map(r=>{if (r != null) r.toString else ""})
                }
                else
                {
                    ret = List("invalid") ::: f.toSeq.toList.map(r=>{if (r != null) r.toString else ""})
                }

            }
            ret
        })

        val result_data = intermediate_data.map(r=> r(0)).countByValue()
        println(result_data)

        result += ("totalCount" -> totalCount.toString)
        result += ("nullCount" -> (if(result_data.get("empty") != None) result_data.get("empty").get.toString else 0))
        result += ("inValidCount" -> (if(result_data.get("invalid") != None) result_data.get("invalid").get.toString else 0))
        result += ("validCount" -> (if(result_data.get("valid") != None) result_data.get("valid").get.toString else 0))
        println(result)
        println("--------宏观数据统计完成")

        val filtered_data:RDD[List[String]] = intermediate_data.filter(list => {
            list(0).equals("invalid")
        })
        val content_data:RDD[List[String]] = filtered_data.map(r=>r.takeRight(r.length-1))
        val sample_data: Array[List[String]] = content_data.take(100)
        sample_data.foreach(r=>{
            val line = (columsList zip r).toMap
            invalidexample += Json(DefaultFormats).write(line)
        })

        println("done")
        println(invalidexample)

        return (result, invalidexample);
    }


}
