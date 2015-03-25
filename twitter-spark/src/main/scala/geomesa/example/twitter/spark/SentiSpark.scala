package geomesa.example.twitter.spark

import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.util.zip.ZipInputStream

import com.beust.jcommander.{JCommander, Parameter}
import com.vividsolutions.jts.geom.{GeometryFactory, Geometry}
import com.vividsolutions.jts.io.{WKTWriter, WKTReader}
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
import org.locationtech.geomesa.tools.commands.FeatureParams
import org.locationtech.geomesa.utils.filters.Filters
import org.opengis.filter.Filter
import org.apache.spark.SparkContext._

class SentiSpark

object SentiSpark {
  def main(args: Array[String]): Unit = {
    new JCommander(TwitterArgs, args.toArray: _*)

    import scala.collection.JavaConversions._

    // Get a handle to the data store
    val params = Map(
      "instanceId" -> TwitterArgs.instance,
      "zookeepers" -> TwitterArgs.zookeepers,
      "user"       -> TwitterArgs.user,
      "password"   -> TwitterArgs.password,
      "tableName"  -> TwitterArgs.catalog)

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

    val f = ff.within(ff.property("geom"), ff.literal(poly))
    val start = ISODateTimeFormat.dateTimeParser().parseDateTime("2014-01-01T00:00:00.000Z")
    val end = ISODateTimeFormat.dateTimeParser().parseDateTime("2014-12-31T23:59:59.999Z")
    val d = ff.during(ff.property("dtg"), Filters.dts2lit(start, end))
    val a = ff.and(f, d)
    val q = new Query(TwitterArgs.featureName, a)

    // Configure Spark
    val conf = new Configuration
    val sconf = GeoMesaSpark.init(new SparkConf(true), ds)
    val sc = new SparkContext(sconf)

    import org.locationtech.geomesa.utils.geotools.Conversions._
    // Create an RDD from a query
    val queryRDD = org.locationtech.geomesa.compute.spark.GeoMesaSpark.rdd(conf, sc, ds, q)

    val geofac = new GeometryFactory()
    val cq = new Query("vacounty", Filter.INCLUDE, Array("COUNTYFP"))
    val countyMap = DataStoreFinder.getDataStore(Map(
      "instanceId" -> TwitterArgs.instance,
      "zookeepers" -> TwitterArgs.zookeepers,
      "user"       -> TwitterArgs.user,
      "password"   -> TwitterArgs.password,
      "tableName"  -> TwitterArgs.countyCatalog))
      .asInstanceOf[AccumuloDataStore]
      .getFeatureSource(TwitterArgs.countyFeature)
      .getFeatures(cq).features()
      .map { sf =>
        (sf.get[String]("COUNTYFP"), sf.geometry)
       }.toSeq.groupBy(_._1)
       .map{ case (c, iter) => (c, iter.map(_._2).reduce(_.union(_))) }

    val countyAndScore = queryRDD.mapPartitions { iter =>
//      val ts = new TwitterSentiment()
//      //val zis = new ZipInputStream(classOf[SentiSpark].getClassLoader.getResourceAsStream("twittwordlist.zip"))
//      val zis = classOf[SentiSpark].getClassLoader.getResourceAsStream("affin.txt")
//      try {
//        ts.load(zis)
//      } finally {
//        IOUtils.closeQuietly(zis)
//      }

      val terms = TwitterArgs.words.split(',')

      iter.flatMap { sf =>
        import org.locationtech.geomesa.utils.geotools.Conversions._
        val text = sf.get[String]("text").toLowerCase

        if (terms.find(text.contains).nonEmpty) {
          val words = text.split("\\W").map(_.trim).toList
//          val score = ts.classify(words)
          countyMap.find( _._2.contains(sf.point)).map{ case (c, geom) => (c, true) }
        } else {
          countyMap.find( _._2.contains(sf.point)).map{ case (c, geom) => (c, false) }
        }
      }
    }

    countyAndScore
      .groupBy(_._1)
      .map { case (c, iter) =>
        val s = iter.toSeq
        val size = s.size
        val numTraffic = s.count(_._2)
        (c,  numTraffic.toDouble / size.toDouble)
       }
      .map { case (c, s) => ((c, countyMap(c)), s) }
      .map { case ((c, g), s) =>
        val wkt = new WKTWriter()
        s"$c\t$s\t${wkt.write(g)}"
      }
      .saveAsTextFile(TwitterArgs.outputDir)
  }

  object TwitterArgs extends FeatureParams {
    @Parameter(names = Array("--output-dir"), description = "hdfs output dir for tsv", required = true)
    var outputDir: String = null

    @Parameter(names = Array("--county-catalog"), description = "county catalog", required = true)
    var countyCatalog: String = null

    @Parameter(names = Array("--county-feature"), description = "county feature", required = true)
    var countyFeature: String = null

    @Parameter(names = Array("--words"), description = "words to filter on, comma separated", required = true)
    var words: String = null
  }

}
