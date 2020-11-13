package com.optum.edp

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatest.mockito.MockitoSugar

class edp_trans_test extends FunSuite with Matchers with BeforeAndAfterAll with MockitoSugar{

	var thriftURL = "thrift://dbsls0324.uhc.com:11016"
	val appName = "sparkTest"
	val master = "local"
	val spark: SparkSession = SparkSession.
		builder().
		master(master).
		appName(appName).
		config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").
		getOrCreate()

	import spark.implicits._

	test("test updateControlTable method for edp_trans class ")
	{
		val controlDF = Seq(("TEST_FEED", Timestamp.valueOf("2020-05-05 23:59:59")
			,Timestamp.valueOf("2020-05-06 23:59:59"))).toDF("feed_name","start_dttm","end_dttm")

		val enddate = "2020-05-06 23:59:59"
		val schema = "systest."
		val controlTable = "trans_cntrl_table"
		val jdbcConnectionString = "jdbc:postgresql://dbvrd28250:5432/postgres"
		val user = "postgres"
		val pass = "postgres"
		val feedName = "TEST_FEED"
		val expected = (Timestamp.valueOf("2020-05-06 23:59:59")
			,Timestamp.valueOf("2020-05-07 23:59:59"))

		assertResult(expected){
			edp_trans.updateControlTable(controlDF,enddate,schema,controlTable
				,jdbcConnectionString,user,pass,feedName)
		}
	}

	test("Test getStartEndDate method for EdpTransComponent class ") {

		val testDataDF = Seq(("TEST_FEED", Timestamp.valueOf("2020-05-05 23:59:59")
			, Timestamp.valueOf("2020-05-06 23:59:59"))).toDF("feed_name", "start_dttm", "end_dttm")

		val expected = ("2020-05-05 23:59:59.0", "2020-05-06 23:59:59.0")

		assertResult(expected) {
			edp_trans.getStartEndDate(testDataDF)
		}

	}

}
