package com.alastor1729.spark


import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import java.io.InputStream
//import scala.io.StdIn


class SparkTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Test-Spark-Local")
      .master("local[*]")
      .getOrCreate()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    // Uncomment the below to leave the spark session open for the UI....
    //StdIn.readLine()
    spark.stop()
    super.afterAll()
  }

    test("empsRDD rowcount") {
        //Note: numSlices is not specified here, so the default parallelism will be used,
        // this is the number of cores if running locally (i.e. local[*]) or the sum of the
        // total number of cores in all the executors in the cluster
        val empsRDD = spark.sparkContext.parallelize(getInputData("/data/employees.json"))
        assert(empsRDD.count === 1000)
    }

    test("titlesRDD rowcount") {
        val titlesRDD = spark.sparkContext.parallelize(getInputData("/data/titles.json"), 5)
        assert(titlesRDD.count === 1470)
    }

    test("titlesDF rowcount") {
        val titlesDF = spark.read.json(getClass.getResource("/data/titles.json").getFile)
        assert(titlesDF.count === 1470)
    }

    test("employeesDF rowcount") {
        val employeesDF = spark.read.json(getClass.getResource("/data/employees.json").getFile)
        assert(employeesDF.count === 1000)
    }

    test("Join employeesDF/titleDF ") {
        val titlesDF = spark.read.json(getClass.getResource("/data/titles.json").getFile)
        val employeesDF = spark.read.json(getClass.getResource("/data/employees.json").getFile)
        val empTitlesDF = employeesDF.join(titlesDF, Seq("emp_no"), "inner")
        assert(empTitlesDF.count === 1470)
    }

    private def getInputData(name: String): Seq[String] = {
        val is: InputStream = getClass.getResourceAsStream(name)
        scala.io.Source.fromInputStream(is).getLines.toSeq
    }


}
