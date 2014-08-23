package geomesa.example.twitter.spark

import java.text.SimpleDateFormat

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{Query, DataStoreFinder}
import org.geotools.factory.CommonFactoryFinder
import org.locationtech.geomesa.compute.spark.GeoMesaSpark
import org.locationtech.geomesa.core.data.AccumuloDataStore

object Runner {
  def main(args: Array[String]) {

    if(args.size != 1) {
      println("1 arg: feature name")
    }
    val feature = args(0)

    import scala.collection.JavaConversions._

    // Get a handle to the data store
    val params = Map(
      "instanceId" -> "dcloud",
      "zookeepers" -> "dzoo1,dzoo2,dzoo3",
      "user"       -> "root",
      "password"   -> "secret",
      "tableName"  -> "geomesa_catalog")

    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

    // Construct a CQL query to filter by bounding box
    val ff = CommonFactoryFinder.getFilterFactory2
    val f = ff.bbox("geom",  -90.32023,38.72009,-90.23957,38.77019, "EPSG:4326")
    val q = new Query(feature, f)

    // Configure Spark
    val conf = new Configuration
    val sconf = GeoMesaSpark.init(new SparkConf(true), ds)
    val sc = new SparkContext(sconf)

    // Create an RDD from a query
    val queryRDD = org.locationtech.geomesa.compute.spark.GeoMesaSpark.rdd(conf, sc, ds, q)

    // Convert RDD[SimpleFeature] to RDD[(String, SimpleFeature)] where the first
    // element of the tuple is the date to the day resolution
    val dayAndFeature = queryRDD.mapPartitions { iter =>
      val df = new SimpleDateFormat("yyyyMMdd")
      val ff = CommonFactoryFinder.getFilterFactory2
      val exp = ff.property("dtg")
      iter.map { f => (df.format(exp.evaluate(f).asInstanceOf[java.util.Date]), f) }
    }

    // Group the results by day
    val groupedByDay = dayAndFeature.groupBy { case (date, _) => date }

    // Count the number of features in each day
    val countByDay = groupedByDay.map { case (date, iter) => (date, iter.size) }

    // Collect the results and print
    countByDay.collect.foreach(println)
  }
}
