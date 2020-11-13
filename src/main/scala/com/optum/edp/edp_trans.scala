package com.optum.edp

import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.optum.exts.cdb.stream.transformer.CDBTransJsonComparator
import org.apache.spark.{SparkConf, sql}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{coalesce, col, current_timestamp, expr, lit, udf}
import org.apache.spark.storage.StorageLevel
import org.postgresql.util.PSQLException

object edp_trans {

	def main(args: Array[String]): Unit = {

		val feed_name = args(0)
		// spark.properties configs
		val conf = new SparkConf()
		val spark = SparkSession.builder().appName("edp_trans").config(conf).enableHiveSupport().getOrCreate()
		val sparksessiontimestamp = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").format(new Date)
		println("Spark Job Started at " + sparksessiontimestamp)
		spark.sparkContext.setLogLevel("ERROR")
		val hdfsServer = spark.conf.get("spark.node.address")
		val jdbcConnectionString = spark.conf.get("spark.jdbcConnectionString")
		val schema = spark.conf.get("spark.schema")
		val user = spark.conf.get("spark.postgresql.user")
		val password = spark.conf.get("spark.postgresql.pwd")
		val ingestion_log = spark.conf.get("spark.ingestion_log")
		val tablesList = spark.conf.get("spark.tablesList")
		val Trans_table = spark.conf.get("spark.Trans_table")
		val controlTable = spark.conf.get("spark.Control_table")
		val jsonPath = spark.conf.get("spark.jsonPath")


		val jsonComp = new CDBTransJsonComparator(jsonPath)

		val jsonprsrUDF = udf((new_val: String, hst_val: String, entity_name: String) => {
			jsonComp.isChange(new_val, hst_val, entity_name)

		})

		println("Printing Variables")
		println("******************************************************************")
		println("Edge Node : " + hdfsServer)
		println("Postgres Server : " + jdbcConnectionString)
		println("Postgres Schema : " + schema)
		println("ingestion_log Table Name : " + ingestion_log)
		println("Feed_name : " + feed_name)
		println("List of tables  : " + tablesList)
		println("Trans_table  : " + Trans_table)
		println("Control_table  : " + controlTable)
		println("******************************************************************")
		println("******************************Get Connection parameter************************************")
		println("************************************************************************************")

		val connectionProperties = new Properties()
		connectionProperties.put("user", user)
		connectionProperties.put("password", password)
		connectionProperties.put("url", jdbcConnectionString)

		val transquery = "(select * from " + schema + controlTable + "  where feed_name='" + feed_name + "') as temp"

		val cntrlDF = spark.read.format("jdbc").option("url", jdbcConnectionString)
			.option("dbtable", transquery).option("user", user).option("password", password).load()

		//Get the control table timestamp
		val (startdate, enddate) = getStartEndDate(cntrlDF)

		println("Control time  startdate :" + startdate + " and endtime :" + enddate)

		val query ="(SELECT * FROM (SELECT table_name as entity_name,row_key,ingestion_ts,row_sts_cd,cdc_flag as change_indicator,cdc_ts,hst_val,new_val,  ROW_NUMBER() OVER ( PARTITION BY row_key,table_name ORDER BY ingestion_ts )min, ROW_NUMBER() OVER ( PARTITION BY row_key,table_name ORDER BY ingestion_ts desc )max from " + schema + ingestion_log + "  where table_name in (" + tablesList + " )AND (INGESTION_TS BETWEEN  '" + startdate + "' AND  '" + enddate + "')) t where max = 1 or min = 1 )  as query"

		println(query)

		val Ingestion_logAllWriteDf: DataFrame = getFinalIngestionLogDF(feed_name, spark, jdbcConnectionString, user, password, jsonprsrUDF, query)

		val sparksessiontimestampNew = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").format(new Date)
		println("Started loading data into EDP_Trans table..........:" + sparksessiontimestampNew)

		try {

			ingestionLogWriter(jdbcConnectionString, schema, user, password, Trans_table, Ingestion_logAllWriteDf)
		}
		catch {
			case ex: PSQLException => println("Exception Caught" + ex.getMessage)

				if (ex.getMessage.toLowerCase.contains("duplicate")) {
					println("Custom Exception Message - Duplicate Key Found ")
					ex.getMessage
					ex.printStackTrace()
				}
				else {
					throw ex;
				}

		} finally {

			println("Finally block after Exception caught while writing !")
		}


		val sparksessiontimestamp6 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS").format(new Date)
		println("Loading completed for Remaining records:" + sparksessiontimestamp6)
		//Contrl table update for next run
		println("*********************************************************************************************")
		println("Updating control table ")


		val (startdateupdt,enddateupdt) = updateControlTable(cntrlDF,enddate,schema,controlTable
			,jdbcConnectionString,user,password,feed_name)


		println("Updating control table completed , new start_date : " + startdateupdt + " and End_date :" + enddateupdt)
		spark.stop()
		println("Finished Spark Process")
		println("************************************************************************************************")
		// main method end
	}


	def updateControlTable(cntrlDF : DataFrame , enddate : String , schema : String , controlTable : String
	                       , jdbcConnectionString: String , user:String ,password:String , feed_name : String ) : (Timestamp,Timestamp) = {
		val enddatenxt = cntrlDF
			.withColumn("end_dttm", col("end_dttm") + expr("INTERVAL 24 HOURS"))
			.select("end_dttm").collectAsList().get(0)(0).toString.trim()
		val strtupdt = enddate
		val startdateupdt = Timestamp.valueOf(strtupdt);
		val endtm1 = enddatenxt
		val enddateupdt = Timestamp.valueOf(endtm1);

		val updatequery = "UPDATE " + schema + controlTable + " SET start_dttm = '" + startdateupdt + "',end_dttm ='" + enddateupdt + "' WHERE feed_name = '" + feed_name + "'"

		val dbc: Connection = DriverManager.getConnection(jdbcConnectionString, user, password)
		var st: PreparedStatement = null
		st = dbc.prepareStatement(updatequery)
		st.addBatch()
		st.executeBatch()
		dbc.close()

		(startdateupdt,enddateupdt)

	}


	def ingestionLogWriter(jdbcConnectionString: String, schema: String, user: String, password: String
	                       , Trans_table: String, Ingestion_logAllWriteDf: DataFrame): Boolean = {
		Ingestion_logAllWriteDf.dropDuplicates("row_key", "entity_name", "datetime")
			.write.mode(SaveMode.Append).format("jdbc").option("url", jdbcConnectionString)
			.option("dbtable", schema + Trans_table).option("numpartitions", 400).option("user", user)
			.option("password", password).option("stringtype", "unspecified").save()

		true
	}

	def getStartEndDate(TransCntrldf:DataFrame): ( String, String) = {

		val startdate = TransCntrldf.select("start_dttm").collectAsList().get(0)(0).toString.trim()
		val enddate = TransCntrldf.select("end_dttm").collectAsList().get(0)(0).toString.trim()

		(startdate, enddate)
	}


	def getFinalIngestionLogDF(feed_name: String, spark: SparkSession, jdbcConnectionString: String, user: String, password: String, jsonprsrUDF: UserDefinedFunction, query: String): sql.DataFrame = {
		val ingestion_logdf = spark.read.format("jdbc").option("url", jdbcConnectionString)
			.option("numpartitions", 50).option("dbtable", query).option("user", user)
			.option("password", password).load()

		ingestion_logdf.persist(StorageLevel.MEMORY_AND_DISK)

		import spark.implicits._
		ingestion_logdf.filter(col("min") === 1).createOrReplaceTempView("leastDf")
		ingestion_logdf.filter(col("max") === 1 && col("min") =!= 1)
			.select('row_key, 'entity_name, 'row_sts_cd, 'new_val).createOrReplaceTempView("latestDf")

		val ingestion_logDFfltrFn1 = spark.sql("select /*+ MAPJOIN(leastDf) */ ldf.row_key , ldf.entity_name ,ldf.change_indicator,CASE WHEN latdf.row_sts_cd is null then ldf.row_sts_cd  else latdf.row_sts_cd end as row_sts_cd, CASE WHEN latdf.row_sts_cd is  null then ldf.new_val  else latdf.new_val  end as new_val ,CASE WHEN latdf.row_sts_cd in('X') then '{}'  else ldf.hst_val  end as hst_val from leastDf ldf left join latestdf latdf on ldf.row_key=latdf.row_key and ldf.entity_name = latdf.entity_name")
		val ingestion_logRmInsertDl = ingestion_logDFfltrFn1.select("*")
			.where("change_indicator != 'I' or row_sts_cd not in ('D','X')")

		val ingestion_logDUdf = ingestion_logRmInsertDl
			.withColumn("feed_name", lit(feed_name + "_FEED"))
			.withColumn("jsonflg", jsonprsrUDF(coalesce(col("new_val"), lit("{}")), coalesce(col("hst_val"), lit("{}")), col("entity_name")))
			.where("jsonflg == 'Y'");

		val Ingestion_logAllWriteDf = ingestion_logDUdf
			.select("feed_name", "row_key", "entity_name", "row_sts_cd", "hst_val")
			.withColumn("datetime", lit(current_timestamp))

		Ingestion_logAllWriteDf
	}

}
