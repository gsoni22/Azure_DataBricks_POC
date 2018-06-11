// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")
val loadData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom18/*")

// COMMAND ----------

display(loadData)

// COMMAND ----------

val row=loadData.select("user_type","distinct_id","lr_birthdate","lr_gender","app_version","duration","duration_seconds","event","manufacturer")

// COMMAND ----------

row.show

// COMMAND ----------

val calcWT=row.filter($"event"==="Video Watched" and $"user_type".isNotNull and $"user_type" =!= "Guest"). select($"distinct_id",$"app_version",$"duration",$"duration_seconds",$"event", when(($"app_version" isin ("47","1.2.16","1.2.21")) and ($"duration" >= 0 and $"duration"<= 36000),"1").
when(!($"app_version" isin ("47","1.2.16","1.2.21")) and ($"duration_seconds" >0 and $"duration_seconds"<= 36000),"0").alias("dws")) .groupBy("distinct_id","dws").agg(when($"dws"==="1",sum("duration")).when($"dws"==="0",sum("duration_seconds"))).drop("dws").withColumnRenamed("CASE WHEN (dws = 1) THEN sum(duration) WHEN (dws = 0) THEN sum(duration_seconds) END","watchTime")

// COMMAND ----------

calcWT.show

// COMMAND ----------

val powerUserFlagTable=calcWT.withColumn("PowerUserFlag",when($"watchTime">1800,1).otherwise(0)).filter($"PowerUserFlag"===1)

// COMMAND ----------

import java.sql.Date._
import java.util.Calendar
import org.apache.spark.sql.types.{DateType, IntegerType}

// COMMAND ----------

val minusUDF=udf((x:Int)=>{
  val sysdate = Calendar.getInstance();
var z=sysdate.get(Calendar.YEAR)
  z-x
})

// COMMAND ----------

val sysdate = Calendar.getInstance();
var x=sysdate.get(Calendar.YEAR)
val gender= loadData.filter($"distinct_id" === powerUserFlagTable.col("distinct_id")).
select($"date_stamp_ist",$"distinct_id",$"lr_gender",$"manufacturer",$"lr_birthdate".cast(DateType),$"region", when($"lr_gender".isNull,"Other").otherwise($"lr_gender"), when($"lr_birthdate".isNotNull,minusUDF(year($"lr_birthdate".cast(DateType))))). drop("lr_gender").withColumnRenamed("CASE WHEN (lr_gender IS NULL) THEN Other ELSE lr_gender END","Gender").withColumnRenamed("CASE WHEN (lr_birthdate IS NOT NULL) THEN UDF(year(cast(lr_birthdate as date))) END","Age").groupBy("date_stamp_ist","manufacturer","region","Gender","Age").agg(countDistinct($"distinct_id")).withColumnRenamed("count(DISTINCT distinct_id)","unique_count")

// COMMAND ----------

display(gender)

// COMMAND ----------

val gen_rdd=gender.rdd

// COMMAND ----------

gen_rdd.collect.foreach(println)

// COMMAND ----------

// Apply script in scala code to shet down the system

// COMMAND ----------

