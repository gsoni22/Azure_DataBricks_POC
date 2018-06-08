// Databricks notebook source
// Set the configration of spark to get the data with get the accesss key
spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")
//spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")
import spark.implicits._
import org.apache.spark.sql.functions._
// val loadData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom18/*")
//val customColSelection=loadData.select('distinct_id,'date_stamp_ist,'date_stamp_ist.alias("date_snap"))
//val loadData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/mixpanel_event_app/")
val loadData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/mixpanel_event_app/")
// val customColSelection1= loadData.select('distinct_id,'date_stamp_ist,'event').repartition(500)
// customColSelection1.cache
// customColSelection1.count

// COMMAND ----------

display(loadData)

// COMMAND ----------

var vists = loadData.select("distinct_id","event","date_stamp_ist").filter($"event" === "App Launched" || $"event" === "App Opened" || $"event" === "App Access" ).groupBy("distinct_id","date_stamp_ist").count().withColumnRenamed("count", "visitors_count")
var views = loadData.select("distinct_id","event","date_stamp_ist").filter($"event" === "mediaReady").groupBy("distinct_id","date_stamp_ist").count().withColumnRenamed("count", "viewers_count")
val result = vists.join(views, usingColumns=Seq("distinct_id","date_stamp_ist"), joinType="outer")

// COMMAND ----------

loadData.select("distinct_id","date_stamp_ist").where("date_stamp_ist >= 2017-12-10 and date_stamp_ist <= 2017-12-20").show

// COMMAND ----------

// Testing
// loadData.createOrReplaceTempView("view1")
// spark.sql("select count(distinct_id) from view1 where (distinct_id = \"006b706b-7de4-48a2-ac1f-37fde6d75f64\" and date_stamp_ist = \"2017-10-01\" and (event = \"App Launched\" or event = \"App Opened\" or event = \"App Access\"))").show

// spark.sql("select count(distinct_id) from view1 where (distinct_id = \"01ba44e3-1bff-4528-888f-fa044d29d2c4\" and date_stamp_ist = \"2017-10-01\" and (event = \"App Launched\" or event = \"App Opened\" or event = \"App Access\"))").show


// spark.sql("select count(distinct_id) from view1 where (distinct_id = \"01ba44e3-1bff-4528-888f-fa044d29d2c4\" and date_stamp_ist = \"2017-10-01\" and (event = \"mediaReady\"))").show

// COMMAND ----------

result.write.saveAsTable("distinct_visit_view_count_tbl_20180418_90days_web")

// COMMAND ----------

//display(dbutils.fs.ls("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/"))
// val loadData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/mixpanel_event_web/")


// COMMAND ----------

import org.apache.spark.sql.types.DateType
import java.text.SimpleDateFormat 
import java.util.Date
import java.time.Period
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.sql.types.{
    StructType, StructField, StringType, IntegerType, DateType}
import org.apache.spark.sql.Row
import spark.implicits._

// COMMAND ----------

// spark.conf.set("spark.driver.maxResultSize", "0")
//spark.conf.set("spark.default.parallelism","300")

// COMMAND ----------

spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")
val Data = spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/mixpanel_event_app/")

// COMMAND ----------

// Data.rdd.getNumPartitions

// COMMAND ----------

val df = Data.select("distinct_id","date_stamp_ist").distinct().repartition(500)

// COMMAND ----------

// df.rdd.getNumPartitions

// COMMAND ----------

// import org.apache.spark.storage.StorageLevel._
// df.persist(MEMORY_AND_DISK_SER)
// df.count()

// COMMAND ----------

// import org.apache.spark.sql.types.DateType
// import java.text.SimpleDateFormat;
// // val evaluateDF=spark.sql("Select * from id_datewise_count_tbl_20180329")
// // evaluateDF.select('distinct_id,$"date_snap".cast(DateType))
// import org.apache.spark.sql._
// import org.apache.spark.sql.functions._
// import org.apache.spark.sql.expressions._
// import java.util.Calendar
// import java.time.LocalDate 
// import java.time.format.DateTimeFormatter

// val schema= StructType(
//     StructField("distinct_id", StringType, true) ::
//     StructField("count", IntegerType, true) :: Nil)
// var res=spark.createDataFrame(sc.emptyRDD[Row], schema)

// var ls= new scala.collection.mutable.ArrayBuffer[List[String]]()
// val byID= Window.partitionBy('distinct_id)
// val partitionedID=df.withColumn("counts", count("date_stamp_ist") over byID)
// partitionedID.cache
// partitionedID.count
// partitionedID.createTempView("calculate_distinct_ids")
// val c1 = Calendar.getInstance();
// val c2 = Calendar.getInstance();
// def computeDistinctIDFunc(from:String,to:String): org.apache.spark.sql.DataFrame={ 
 
// var startDate = from
// val formatter =new SimpleDateFormat("yyyy-MM-dd");
// val oldDate = formatter.parse(startDate)
// val toDate = to
// val newDate = formatter.parse(toDate)
// val numberOfDays=((newDate.getTime-oldDate.getTime)/(1000*60*60*24)).toInt


// for (i<-1 to numberOfDays){
    
//   val initialDate=startDate
//   val fromDate = formatter.parse(initialDate)
//   var counter=0
//   for (j<-i+1 to numberOfDays){
// //     val endDay=fromDate.plusDays(1).getDayOfMonth.toInt
//    // println(i,j)
//     c2.setTime(fromDate)
//     c2.add(Calendar.DATE,counter+1)
  
//     val result=spark.sql("select distinct")
    
// //     val endDate=formatter.format(c2.getTime())
// // //     val endDateQuery=formatter.format(endDate)
// //   //  println(initialDate,endDate)
// //    val distinctCount=spark.sql(s"""Select distinct_id from calculate_distinct_ids where date_stamp_ist >="$initialDate" and date_stamp_ist<="$endDate"   """).groupBy('distinct_id).agg(count('distinct_id)).filter($"count(distinct_id)">=1).count()
// //     res=res.union(distinctCount)
// //    // val distinctCount=spark.sql(s"""Select distinct_id from calculate_distinct_ids where date_snap=="$initialDate"   """).groupBy('distinct_id).agg(count('distinct_id)).filter($"count(distinct_id)">=1).count()
// // // sc.parallelize(Seq((initialDate,endDate,distinctCount))).toDF("from","to","distinct_count").write.format("parquet").mode(SaveMode.Append).saveAsTable("distincts_count_tbl_"+java.time.LocalDate.now.toString.replace("-",""))
    
    
// ls+=List(initialDate,endDate,distinctCount.toString())
//     counter=counter+1 
//   }
// c1.setTime(fromDate)
// c1.add(Calendar.DATE,1)
// startDate=formatter.format(c1.getTime())
// }
// res
// }

// COMMAND ----------

def fun(v1:String, v2:String, df: org.apache.spark.sql.DataFrame ): org.apache.spark.sql.DataFrame ={
  val df1 = df.filter((df("date_stamp_ist") >= v1) and (df("date_stamp_ist") <=v2))
                .groupBy("distinct_id").count()
                .withColumn("StartDate",lit(v1)).withColumn("EndDate",lit(v2))
  df1
}

// COMMAND ----------

val start="2017-12-20"
//val end="2017-12-04"
val end="2018-01-10"
val startDate=LocalDate.parse(start)
val endDate=LocalDate.parse(end)
val count= ChronoUnit.DAYS.between(startDate, endDate)+1
var r=(0 until count.toInt).map(startDate.plusDays(_))

// COMMAND ----------



// COMMAND ----------

//Schema design for appending Dataframe values
val schema= StructType(
    StructField("startDate", StringType, true) ::
    StructField("endDate", StringType, true) ::
    StructField("count", IntegerType, true) :: Nil)
var temp=spark.createDataFrame(sc.emptyRDD[Row], schema)
var mydf=temp

// COMMAND ----------

//To generate all possible permutation of dates of single dataframe 
var inner=0
var w=0
var count2=0
for (w<-0 until r.length){
  for (inner <- w until r.length){
    mydf=fun(r(w).toString(),r(inner).toString(),df).groupBy("startDate","endDate").count()
    temp=temp.union(mydf)
 }
}

// COMMAND ----------

val start1="2017-12-01"
val end1="2017-01-20"
val startDate1=LocalDate.parse(start1)
val endDate1=LocalDate.parse(end1)
val count1= ChronoUnit.DAYS.between(startDate1, endDate1)
var r1=(0 until count.toInt).map(startDate1.plusDays(_))

// COMMAND ----------

var inner=0
var w=0
var count2=0
for (w<-0 until r.length){
  for (inner <- 0 until r1.length){
    mydf=fun(r(w).toString(),r1(inner).toString(),df).groupBy("startDate","endDate").count()
    temp=temp.union(mydf)
 }
}
//

// COMMAND ----------

 temp.rdd.getNumPartitions

// COMMAND ----------

val temp1=temp.coalesce(500)

// COMMAND ----------

// temp.count

// COMMAND ----------

temp1.cache()
temp1.show()

// COMMAND ----------

temp1.write.mode(SaveMode.Append).saveAsTable("distinct_id_count_tbl_20To40_days")

// COMMAND ----------

// import org.apache.spark.util.SizeEstimator
// print(SizeEstimator.estimate(bnn))

// COMMAND ----------

// dbutils.fs.rm("/tmp/hive",true)

// COMMAND ----------

//NEW

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql.functions._

// COMMAND ----------

spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")
val loadData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/mixpanel_event_app/").select("distinct_id","date_stamp_ist","event").repartition(500)

// COMMAND ----------

val newData=loadData.select($"*", when($"event" isin("App Launched","App Opened", "App Access"),0.01).when($"event" isin("mediaReady"),0.0001).otherwise(0).alias("strike")).drop("event").groupBy("distinct_id","date_stamp_ist","strike").agg(sum($"strike"))

// COMMAND ----------

newData.cache
display(newData)

// COMMAND ----------

newData.withColumnRenamed("sum(strike)","sum_strike").write.saveAsTable("distinct_id_visitor_viewer_strike_90days")

// COMMAND ----------

//ORGANIC_INORGANIC

// COMMAND ----------

val newData=loadData.select($"distinct_id",$"date_stamp_ist",$"media_source", when($"media_source" isin("Organic"),1).otherwise(0).alias("class")).drop("media_source").groupBy("distinct_id","date_stamp_ist","class").count
newData.show

// COMMAND ----------

newData.write.saveAsTable("ORGANIC_INORGANIC_INSTALL_FLAG_Organic1_Inorganic0")

// COMMAND ----------

//VIDEO_WATCHED

// COMMAND ----------

"IF app_version in (47,1.2.16, 1.2.21) and duration between 0 to 36000 and event = 'Video Watched'
THEN (SUM (duration) 
ELSE IF app_version NOT in (47,1.2.16, 1.2.21) and duration_seconds between 0 to 36000 and event = 'Video Watched'
THEN (SUM (duration_seconds)"


// COMMAND ----------

val DURATION_WATCHED_SECS = loadData.select($"distinct_id",$"date_stamp_ist",$"event",$"duration", $"app_version", $"duration_seconds", when($"app_version" isin("47","1.2.16","1.2.21") and $"duration" >= 0  and $"duration" <= 36000 and $"event" === "Video Watched" ,1).when(!($"app_version" isin("47","1.2.16","1.2.21")) and $"duration_seconds" >= 0  and $"duration_seconds" <= 36000 and $"event" === "Video Watched" ,0 ).alias("duration_flag")).filter($"event" === "Video Watched").drop("event").filter("not duration_flag is null").drop("app_version")

// COMMAND ----------

DURATION_WATCHED_SECS.cache
display(DURATION_WATCHED_SECS)

// COMMAND ----------

val test_duration=DURATION_WATCHED_SECS.filter($"duration_flag"==="1")

// COMMAND ----------

test_duration.count

// COMMAND ----------

val d=DURATION_WATCHED_SECS.where("duration_flag==1").groupBy($"distinct_id",$"date_stamp_ist",$"duration_flag").agg(sum($"duration"))

// COMMAND ----------

val ds=DURATION_WATCHED_SECS.where("duration_flag==0").groupBy($"distinct_id",$"date_stamp_ist",$"duration_flag").agg(sum($"duration_seconds"))

// COMMAND ----------

println(d.count)
println(ds.count)

// COMMAND ----------

val duration_watched_second=d.union(ds)

// COMMAND ----------

display(duration_watched_second)

// COMMAND ----------

duration_watched_second.write.format("csv").option("headers","true").saveAsTable("Final_Final_Duration_watched_sec_correct")

// COMMAND ----------

display(res102)

// COMMAND ----------

d.count

// COMMAND ----------

ds.count

// COMMAND ----------

val duration_final=d.join(ds,usingColumns=Seq("distinct_id","date_stamp_ist"), joinType="outer").drop("duration_flag")

// COMMAND ----------

duration_final.cache
duration_final.count

// COMMAND ----------

display(duration_final)

// COMMAND ----------

val duration_final1=duration_final.withColumn("sum_duration",coalesce($"sum(duration)",$"sum(duration_seconds)")).drop("sum(duration)").drop("sum(duration_seconds)")

// COMMAND ----------

duration_final1.cache()
duration_final1.show

// COMMAND ----------

val dws=spark.read.table("DURATION_WATCHED_SECS")

// COMMAND ----------



// COMMAND ----------

duration_final1.withColumnRenamed("sum(duration)","sum_duration").write.mode(SaveMode.Overwrite).saveAsTable("DURATION_WATCHED_SECS")

// COMMAND ----------

duration_final1.count

// COMMAND ----------

wsc.select("sum_duration").agg(max($"sum_duration"))

// COMMAND ----------

val num_viewers=spark.read.table("distinct_visit_view_count_tbl_20180418_90days_app")

// COMMAND ----------

val joinDF=num_viewers.select("distinct_id", "date_stamp_ist", "viewers_count").join(duration_final1, usingColumns=Seq("distinct_id","date_stamp_ist"), joinType="inner")

// COMMAND ----------

joinDF.cache
joinDF.show

// COMMAND ----------

joinDF.count

// COMMAND ----------

val tsv=joinDF.withColumn("tsv",($"sum_duration"/$"viewers_count"))

// COMMAND ----------

tsv.cache
tsv.show

// COMMAND ----------

tsv.write.saveAsTable("TSV")

// COMMAND ----------

val tsv=spark.read.table("TSV").count

// COMMAND ----------

tsv.withColumn("tsv1",round)

// COMMAND ----------

num_viewers.count

// COMMAND ----------

num_viewers.filter($"viewers_count".isNotNull).count

// COMMAND ----------

num_viewers.filter($"viewers_count".isNotNull).count

// COMMAND ----------

// MAGIC %sql alter table distinct_visit_view_count_tbl_20180418_90days_app rename to NUM_VISITORS_VIEWERS

// COMMAND ----------

loadData.select("os","os_version").limit(10).show

// COMMAND ----------

loadData.printSchema

// COMMAND ----------

val osDF=loadData.select("distinct_id","os").filter($"os".isNotNull).groupBy("os").count()

// COMMAND ----------

osDF.cache
osDF.show

// COMMAND ----------

//new

// COMMAND ----------

spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")
import spark.implicits._
import org.apache.spark.sql.functions._
val loadData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/mixpanel_event_app/").select("distinct_id", "date_stamp_ist", "event","duration", "duration_seconds","app_version")

// COMMAND ----------

val newData=loadData.select($"*", when($"event" isin("App Launched","App Opened", "App Access"),0.01).when($"event" isin("mediaReady"),0.0001).otherwise(0).alias("score"),when($"app_version" isin("47","1.2.16","1.2.21") and $"duration" >= 0  and $"duration" <= 36000 and $"event" === "Video Watched" ,1).when(!($"app_version" isin("47","1.2.16","1.2.21")) and $"duration_seconds" >= 0  and $"duration_seconds" <= 36000 and $"event" === "Video Watched" ,0 ).alias("duration_flag"))

// COMMAND ----------

newData.show

// COMMAND ----------

loadData.select("distinct_id","event").groupBy("event","distinct_id").count()

// COMMAND ----------

res6.show

// COMMAND ----------

loadData.select("distinct_id","event").groupBy("distinct_id").agg(collect_list("event"))

// COMMAND ----------

display(res10)

// COMMAND ----------

sqlContext.clearCache()

// COMMAND ----------

val gsdf=loadData.select($"date_stamp_ist",$"media_source",$"os",$"interface",when($"media_source" isin("Organic"),1).otherwise(0)) .groupBy("date_stamp_ist","media_source","os","interface").count

// COMMAND ----------

loadData.show(truncate=false)

// COMMAND ----------

val orgdata=spark.read.table("organic_inorganic_install_flag_organic1_inorganic0")

// COMMAND ----------

display(orgdata.groupBy(,"date_stamp_ist","class").count())

// COMMAND ----------

//val DURATION_WATCHED_SECS = loadData.select($"distinct_id",$"date_stamp_ist",$"event",$"duration", $"app_version", $"duration_seconds", when($"app_version" isin("47","1.2.16","1.2.21") and $"duration" >= 0  and $"duration" <= 36000 and $"event" === "Video Watched" ,1).when(!($"app_version" isin("47","1.2.16","1.2.21")) and $"duration_seconds" >= 0  and $"duration_seconds" <= 36000 and $"event" === "Video Watched" ,0 ).alias("duration_flag")).filter($"event" === "Video Watched").drop("event").filter("not duration_flag is null").drop("duration").drop("duration_flag").drop("app_version")


//val poweruserDF=DURATION_WATCHED_SECS.select($"distinct_id",$"date_stamp_ist",$"duration_seconds", when($"duration_seconds"/60 >=30,"Y").otherwise("N"). alias("daily_power_user"),when($"duration_seconds"/60 >= 30 and $"duration_seconds"/60 < 40,">=30Mins").when($"duration_seconds"/60 >=40 and $"duration_seconds"/60 < 50,">=40Mins").when($"duration_seconds"/60 >= 50 and $"duration_seconds"/60 < 80,">=50 Mins").when($"duration_seconds"/60 >= 80,"80 Min").otherwise("N").alias("duration_power_user_flag")).drop("distinct_id")

//val gsdf=loadData.select($"date_stamp_ist",$"first_app_launch_date".cast(DateType),$"media_source",$"os",$"user_type", when($"media_source"==="Organic",1) .otherwise(0).alias("Lead"),when($"user_type" =!= "null" && $"user_type"=!= "Guest","REGISTERED").when($"date_stamp_ist" === $"first_app_launch_date". cast(DateType),"NEW USER").otherwise("NOT REGISTERED") .alias("userType")).drop("user_type").drop("first_app_launch_date").drop("media_source")

// New user and repeated use Flag 
//IF date_stamp = first_app_launch_date THEN the distinct_id should be tagged as NEW user thro'out the day.
val repeated_user=loadData.select($"distinct_id",$"first_app_launch_date".cast(DateType),$"date_stamp_ist",when($"date_stamp_ist"===$"date_stamp_ist" ))
//repeated_user.count//$"date_stamp_ist",$"first_app_launch_date".cast(DateType),$""

// COMMAND ----------

dws.show

// COMMAND ----------

// Onea copy in the main changes

val aaa= loadData.select($"distinct_id",$"date_stamp_ist",$"event",$"duration",$"app_version",$"duration_seconds",$"media_source",$"os",$"user_type", $"first_app_launch_date".cast(DateType))

val nnn= aaa.join(dws,usingColumns=Seq("distinct_id","date_stamp_ist"),joinType="outer")
//val result = vists.join(views, usingColumns=Seq("distinct_id","date_stamp_ist"), joinType="outer")
//val aaa=loadData.select($"distinct_id",$"date_stamp_ist",$"duration_seconds",$"os",$"user_type",$"first_app_launch_date".cast(DateType),when($"os".isNull,"others").otherwise($"os").alias("operatingSystem"), when($"duration_seconds"/60 >=30,"Y").otherwise("N"). alias("daily_power_user"),when($"duration_seconds"/60 >= 30 and $"duration_seconds"/60 < 40,">=30Mins").when($"duration_seconds"/60 >=40 and $"duration_seconds"/60 < 50,">=40Mins").when($"duration_seconds"/60 >= 50 and $"duration_seconds"/60 < 80,">=50 Mins").when($"duration_seconds"/60 >= 80,"80 Min").otherwise("N").alias("duration_power_user_flag"), when($"media_source"==="Organic","Organic").otherwise("Inorganic").alias("Lead") ,when($"user_type" =!= "null" && $"user_type"=!= "Guest","REGISTERED").when($"date_stamp_ist" === $"first_app_launch_date". cast(DateType),"NEW USER").otherwise("NOT REGISTERED").alias("userType"))//.drop("event").drop("duration").drop("duration_flag").drop("app_version").drop("user_type").drop("first_app_launch_date").drop("distinct_id").drop("duration_seconds").drop("media_source").drop("os").groupBy("date_stamp_ist","operatingSystem","daily_power_user","duration_power_user_flag","Lead","userType").count

// COMMAND ----------

nnn.show

// COMMAND ----------

val aaa= loadData.select($"distinct_id",$"date_stamp_ist",$"event",$"duration",$"app_version",$"duration_seconds",$"media_source",$"os",$"user_type", $"first_app_launch_date".cast(DateType))

val nnn= aaa.join(dws,usingColumns=Seq("distinct_id","date_stamp_ist"),joinType="outer")

val mmmr= nnn.select($"distinct_id",$"date_stamp_ist",$"event",$"duration",$"app_version",$"duration_seconds",$"media_source",$"os",$"user_type", $"first_app_launch_date".cast(DateType),$"sum_duration",when($"os".isNull,"others").otherwise($"os").alias("operatingSystem"), when($"sum_duration"/60 >=30,"Y").otherwise("N"). alias("daily_power_user"),when($"sum_duration"/60 >= 30 and $"sum_duration"/60 < 40,">=30Mins").when($"sum_duration"/60 >=40 and $"sum_duration"/60 < 50,">=40Mins").when($"sum_duration"/60 >= 50 and $"sum_duration"/60 < 80,">=50 Mins").when($"sum_duration"/60 >= 80,"80 Min").otherwise("N").alias("duration_power_user_flag"), when($"media_source"==="Organic","Organic").otherwise("Inorganic").alias("Lead") ,when($"user_type" =!= "null" && $"user_type"=!= "Guest","REGISTERED").otherwise("NOT Register").alias("Status"),when($"date_stamp_ist" === $"first_app_launch_date".cast(DateType),"NEW USER").alias("New or Not")).drop("event").drop("duration").drop("duration_flag").drop("app_version"). drop("user_type").drop("first_app_launch_date").drop("distinct_id").drop("duration_seconds").drop("media_source").drop("os").groupBy("date_stamp_ist","operatingSystem","daily_power_user","duration_power_user_flag","Lead","Status","New or Not").count

// COMMAND ----------

mmmr.agg(sum($"count")).show

// COMMAND ----------

loadData.count

// COMMAND ----------

display(mmmr)

// COMMAND ----------

mmmr.count

// COMMAND ----------

mmmr.coalesce(1).write.format("csv").option("header","true").save("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/pweruserwith_newfor")

// COMMAND ----------

val xyz=loadData.select($"distinct_id",$"date_stamp_ist",$"event",$"duration",$"app_version",$"duration_seconds",$"media_source",$"os",$"user_type", $"first_app_launch_date".cast(DateType),when($"os".isNull,"others").otherwise($"os").alias("operatingSystem"),when($"app_version" isin("47","1.2.16","1.2.21") and $"duration" >= 0  and $"duration" <= 36000 and $"event" === "Video Watched" ,1).when(!($"app_version" isin("47","1.2.16","1.2.21")) and $"duration_seconds" >= 0  and $"duration_seconds" <= 36000 and $"event" === "Video Watched" ,0).alias("duration_flag"), when($"duration_seconds"/60 >=30,"Y").otherwise("N"). alias("daily_power_user"),when($"duration_seconds"/60 >= 30 and $"duration_seconds"/60 < 40,">=30Mins").when($"duration_seconds"/60 >=40 and $"duration_seconds"/60 < 50,">=40Mins").when($"duration_seconds"/60 >= 50 and $"duration_seconds"/60 < 80,">=50 Mins").when($"duration_seconds"/60 >= 80,"80 Min").otherwise("N").alias("duration_power_user_flag"), when($"media_source"==="Organic","Organic").otherwise("Inorganic").alias("Lead") ,when($"user_type" =!= "null" && $"user_type"=!= "Guest","REGISTERED").when($"date_stamp_ist" === $"first_app_launch_date". cast(DateType),"NEW USER").otherwise("NOT REGISTERED").alias("userType"))//.drop("event").drop("duration").drop("duration_flag").drop("app_version").drop("user_type").drop("first_app_launch_date").drop("distinct_id").drop("duration_seconds").drop("media_source").drop("os").groupBy("date_stamp_ist","operatingSystem","daily_power_user","duration_power_user_flag","Lead","userType").count


//val poweruserDF=DURATION_WATCHED_SECS.select($"distinct_id",$"date_stamp_ist",$"duration_seconds", when($"duration_seconds"/60 >=30,"Y").otherwise("N"). alias("daily_power_user"),when($"duration_seconds"/60 >= 30 and $"duration_seconds"/60 < 40,">=30Mins").when($"duration_seconds"/60 >=40 and $"duration_seconds"/60 < 50,">=40Mins").when($"duration_seconds"/60 >= 50 and $"duration_seconds"/60 < 80,">=50 Mins").when($"duration_seconds"/60 >= 80,"80 Min").otherwise("N").alias("duration_power_user_flag")).drop("distinct_id")

//val gsdf=loadData.select($"media_source",$"os",$"user_type",$"first_app_launch_date".cast(DateType), when($"media_source"==="Organic",1) .otherwise(0).alias("Lead"),when($"user_type" =!= "null" && $"user_type"=!= "Guest","REGISTERED").when($"date_stamp_ist" === $"first_app_launch_date". cast(DateType),"NEW USER").otherwise("NOT REGISTERED") .alias("userType")).drop("user_type").drop("first_app_launch_date").drop("media_source")

//.drop("event").drop("duration").drop("duration_flag").drop("app_version").drop("user_type").drop("first_app_launch_date").drop("distinct_id").drop("duration_seconds").drop("media_source")

// COMMAND ----------

xyz.cache
xyz.count

// COMMAND ----------

xyz.select($"count").agg(sum($"count")).first.getLong(0)

// COMMAND ----------

xyz.coalesce(1).write.format("csv").option("header","true").save("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/pweruser")


// COMMAND ----------

// MAGIC %fs ls

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/",
  mountPoint = "/mnt/blob",
  extraConfigs = Map("fs.azure.account.key.cdcvillx170@strgt000000mp.blob.core.windows.net" -> "5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ=="))

// COMMAND ----------

val df=spark.read.option("header","true").csv("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/Gaurav.csv")

// COMMAND ----------

df.show

// COMMAND ----------

dbutils.fs.ls("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/")

// COMMAND ----------

val date_wise_os_lead=gsdf.groupBy("date_stamp_ist","os","Lead","userType").count

// COMMAND ----------

date_wise_os_lead.write.saveAsTable("Date_OS_UserType_Lead")

// COMMAND ----------

val bnv= loadData.select($"distinct_id",$"date_stamp_ist",$"event",$"duration",$"duration_seconds",$"app_version",$"first_app_launch_date",$"media_source",$"os",$"user_type")

// COMMAND ----------

display(bnv)

// COMMAND ----------

xyz.printSchema

// COMMAND ----------

xyz.coalesce(1).write.format("csv").save("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacomOutput/Gaurav.csv")

// COMMAND ----------

xyz.write.saveAsTable("os_power_user_duration_lead_tbl")

// COMMAND ----------

val tsv=spark.read.table("tsv")
tsv.coalesce(1).write.option("header","true").format("csv").save("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacomOutput/tsv_final.csv")

// COMMAND ----------

// App installed Logic Implimentation 
val app_installed_DF=loadData.select($"distinct_id",$"event",$"date_stamp_ist",when($"event" === "App Install","Installed").otherwise("Not installed").alias("App Status")).filter($"App Status"==="Installed").drop("event").groupBy("distinct_id","date_stamp_ist").count

// COMMAND ----------

display(app_installed_DF)

// COMMAND ----------

//val appinstallsDistinct=app_installed_DF.select('date_stamp_ist,when($"count">=1,1).otherwise(0).alias("counter")).groupBy("date_stamp_ist").count()
appinstallsDistinct.coalesce(1).write.format("csv").option("header","true").save("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacomOutput/app_installs_90_correct_final")

// COMMAND ----------

appinstallsDistinct.printSchema

// COMMAND ----------

val tsv=spark.read.table("tsv")
val tsv1=tsv.select($"tsv",$"date_stamp_ist").groupBy("date_stamp_ist").agg((sum($"tsv"))/count($"date_stamp_ist")).withColumnRenamed("(sum(tsv) / count(date_stamp_ist))","TSV")

// COMMAND ----------

tsv1.cache
tsv1.show

// COMMAND ----------

display(tsv1)

// COMMAND ----------

display(tsv1)

// COMMAND ----------

tsv1.coalesce(1).write.format("csv").option("header","true").save("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacomOutput/tsv_sum1")

// COMMAND ----------

val dist_viewer=loadData.select($"distinct_id","event", when($"event"=== "mediaReady")).distinct().groupBy("date_stamp_ist")

// COMMAND ----------

var vists = loadData.select("distinct_id","event","date_stamp_ist").filter($"event" === "App Launched" || $"event" === "App Opened" || $"event" === "App Access" ).distinct().drop("distinct_id").groupBy("date_stamp_ist").count().withColumnRenamed("count", "visitors_count")

var views = loadData.select("distinct_id","event","date_stamp_ist").filter($"event" === "mediaReady").distinct().groupBy("distinct_id","date_stamp_ist").count(). withColumnRenamed("count", "viewers_count")

//val result = vists.join(views, usingColumns=Seq("distinct_id","date_stamp_ist"), joinType="outer")

// COMMAND ----------

vists.coalesce(1).write.format("csv").option("header","true").save("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacomOutput/visitors_data")


// COMMAND ----------

var views = loadData.select("distinct_id","event","date_stamp_ist").filter($"event" === "mediaReady").distinct().groupBy("date_stamp_ist").count().withColumnRenamed("count", "viewers_count")

// COMMAND ----------

views.first

// COMMAND ----------

views.coalesce(1).write.format("csv").option("header","true").save("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacomOutput/viewers_data")

// COMMAND ----------

views.count

// COMMAND ----------

loadData.createOrReplaceTempView("myview")
val distinct_users=spark.sql("select count (distinct distinct_id),date_stamp_ist from myview group by date_stamp_ist")

// COMMAND ----------

distinct_users.show

// COMMAND ----------

distinct_users.write.option("header","true").format("csv").save("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacomOutput/distinct_users_count")

// COMMAND ----------

display(distinct_users)

// COMMAND ----------

dbutils.fs.rm("/tmp",true)

// COMMAND ----------

dws.agg(max($"sum_duration"),min($"sum_duration")).show(truncate=false)

// COMMAND ----------



// COMMAND ----------

spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")
//spark.conf.set("fs.azure.account.key.strgt000000mp.blob.core.windows.net","5kXYE0csbK/4W8xrPUjv3x1ywsNt7DhcFZEDp6Ly0/kB6J4duvuZSpgLo5h3oh2Wh/OKkq2ERppkcw+8xIDFoQ==")
import spark.implicits._
import org.apache.spark.sql.functions._
// val loadData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom18/*")
//val customColSelection=loadData.select('distinct_id,'date_stamp_ist,'date_stamp_ist.alias("date_snap"))
//val loadData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/mixpanel_event_app/")
val originalData=spark.read.parquet("wasbs://cdcvillx170@strgt000000mp.blob.core.windows.net/viacom90days/mixpanel_event_app/")

// COMMAND ----------

originalData.select("duration","duration_seconds").show

// COMMAND ----------

val power_userJoin = loadData.join(dws, usingColumns=Seq("distinct_id","date_stamp_ist"), joinType="inner")

// COMMAND ----------

loadData.count

// COMMAND ----------

power_userJoin.count

// COMMAND ----------

power_userJoin.filter()

// COMMAND ----------

val selDF=loadData.select("distinct_id","date_stamp_ist").filter($"event"==="mediaReady")
val joinDF=selDF.join(dws, usingColumns=Seq("distinct_id","date_stamp_ist"), joinType="inner")

// COMMAND ----------

joinDF.filter($"sum_duration">=1800)

// COMMAND ----------

res64.count

// COMMAND ----------

val powerDF=res64.withColumn("date_snap", to_date($"date_stamp_ist")).drop("date_stamp_ist")

// COMMAND ----------

import org.apache.spark.sql.types.DateType
import java.text.SimpleDateFormat;
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import java.util.Calendar
import java.time.LocalDate 
import java.time.format.DateTimeFormatter

var power_list= new scala.collection.mutable.ArrayBuffer[List[String]]()
 val byID= Window.partitionBy('distinct_id)
 val partitionedID=powerDF.withColumn("counts", count("date_snap") over byID)
 partitionedID.cache
 partitionedID.count
 partitionedID.createOrReplaceTempView("power_view")
val c1 = Calendar.getInstance();
val c2 = Calendar.getInstance();
def computeDistinctIDFunc(from:String,to:String)={ 
 
var startDate = from
val formatter =new SimpleDateFormat("yyyy-MM-dd");
val oldDate = formatter.parse(startDate)
val toDate = to
val newDate = formatter.parse(toDate)
val numberOfDays=((newDate.getTime-oldDate.getTime)/(1000*60*60*24)).toInt


for (i<-1 to numberOfDays){
    
  val initialDate=startDate
  val fromDate = formatter.parse(initialDate)
  var counter=0
  for (j<-i to numberOfDays){
//     val endDay=fromDate.plusDays(1).getDayOfMonth.toInt
    //println(i,j)
    c2.setTime(fromDate)
    c2.add(Calendar.DATE,counter+1)
  
    val endDate=formatter.format(c2.getTime())
//     val endDateQuery=formatter.format(endDate)
   println(initialDate,endDate)
  //  val distinctCount=spark.sql(s"""Select `sum(visit_flag)`, `sum(viewer_flag)` from visit_view where date_snap >="$initialDate" and date_snap<="$endDate"   """).groupBy('distinct_id).agg(count('distinct_id)).filter($"count(distinct_id)">=1).count()
    val count=spark.sql(s"""select distinct_id from power_view where date_snap >="$initialDate" and date_snap<="$endDate" """).distinct.count()

    
    power_list+=List(initialDate,endDate,count.toString)
    counter=counter+1 
  }
c1.setTime(fromDate)
c1.add(Calendar.DATE,1)
startDate=formatter.format(c1.getTime())
}
}

// COMMAND ----------

spark.sql(s"""select distinct_id from power_view where date_snap >=cast('2017-12-01' as date) and date_snap <= cast('2018-02-28' as date) """).distinct.count()

// COMMAND ----------

computeDistinctIDFunc("2017-12-01","2018-03-01")

// COMMAND ----------

power_list.length

// COMMAND ----------

val df=sc.parallelize(power_list).toDF("value_arr")


val result= df.select($"value_arr".getItem(0).alias("startDate"),$"value_arr".getItem(1).alias("endDate"),$"value_arr".getItem(2).alias("power_user_count"))

// COMMAND ----------

display(result)

// COMMAND ----------

result.write.saveAsTable("power_user_count_range")