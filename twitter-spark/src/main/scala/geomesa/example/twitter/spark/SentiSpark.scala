package geomesa.example.twitter.spark

import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.zip.ZipInputStream

import com.vividsolutions.jts.io.WKTReader
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkContext, SparkConf}
import org.geotools.data.{Query, DataStoreFinder}
import org.geotools.factory.CommonFactoryFinder
import org.geotools.filter.text.ecql.ECQL
import org.geotools.temporal.`object`.{DefaultPosition, DefaultPeriod}
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormat}
import org.locationtech.geomesa.compute.spark.GeoMesaSpark
import org.locationtech.geomesa.core.data.AccumuloDataStore
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.utils.filters.Filters

class SentiSpark

object SentiSpark {
  def main(args: Array[String]): Unit = {

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
      "tableName"  -> "twitter2014st")

    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]

    // Construct a CQL query to filter by bounding box (set bbox based on data we collected)
    val ff = CommonFactoryFinder.getFilterFactory2
    val pis = classOf[SentiSpark].getClassLoader.getResourceAsStream("va.poly.wkt")
    val poly =
    try {
      new WKTReader().read(IOUtils.toString(pis))
    } finally {
      IOUtils.closeQuietly(pis)
    }
    //val f = ff.bbox("geom", -90.32023, 38.72009, -90.23957, 38.77019, "EPSG:4326")
    val f = ff.within(ff.property("geom"), ff.literal(poly))
    val start = ISODateTimeFormat.dateTimeParser().parseDateTime("2014-07-01T00:00:00.000Z")
    val end = ISODateTimeFormat.dateTimeParser().parseDateTime("2014-07-01T23:59:59.999Z")
    val wildcard = "text like '%fuck%'"
    val d = ff.during(ff.property("dtg"), Filters.dts2lit(start, end))
    //val a = ff.and(f, ECQL.toFilter(wildcard))
    //val a = ff.and(List(f, d, ECQL.toFilter(wildcard)))
    val a = ff.and(f, d)
    val q = new Query(feature, a)

    // Configure Spark
    val conf = new Configuration
    val sconf = GeoMesaSpark.init(new SparkConf(true), ds)
    val sc = new SparkContext(sconf)

    // Create an RDD from a query
    val queryRDD = org.locationtech.geomesa.compute.spark.GeoMesaSpark.rdd(conf, sc, ds, q)

    queryRDD.mapPartitions { iter =>
      val ts = new TwitterSentiment()
      //val zis = new ZipInputStream(classOf[SentiSpark].getClassLoader.getResourceAsStream("twittwordlist.zip"))
      val zis = classOf[SentiSpark].getClassLoader.getResourceAsStream("affin.txt")
      try {
        ts.load(zis)
      } finally {
        IOUtils.closeQuietly(zis)
      }
      iter.flatMap { sf =>
        import org.locationtech.geomesa.utils.geotools.Conversions._
        val text = sf.get[String]("text")
        if (Pattern.compile("traffic").matcher(text).find()) {
          val words = text.split("\\W").map(_.trim).toList
          val score = ts.classify(words)
          Some(words.toString, score)
        } else {
          None
        }
      }
    }.foreach{ case (s, d) => println(s"$d $s")}

    // Group the results by day
    //val groupedByDay = dayAndFeature.groupBy { case (date, _) => date }

    // Count the number of features in each day
    //val countByDay = groupedByDay.map { case (date, iter) => (date, iter.size) }

    // Collect the results and print
    //countByDay.collect.sortBy(_._1)foreach(println)
  }
}
