package geomesa.example.twitter.spark

import com.beust.jcommander.{JCommander, Parameter}
import com.vividsolutions.jts.io.{WKTReader, WKTWriter}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, Query}
import org.geotools.factory.CommonFactoryFinder
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.compute.spark.GeoMesaSpark
import org.locationtech.geomesa.tools.commands.FeatureParams
import org.locationtech.geomesa.utils.filters.Filters
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter


class CountyAgg

object CountyAgg {
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
    val pis = classOf[CountyAgg].getClassLoader.getResourceAsStream("va.poly.wkt")
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
    val queryRDD = org.locationtech.geomesa.compute.spark.GeoMesaSpark.rdd(conf, sc, params, q)

    val cq = new Query(TwitterArgs.countyFeature, Filter.INCLUDE, Array("COUNTYFP"))
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

    val terms = TwitterArgs.words.split(',')
    val outdir = TwitterArgs.outputDir

    def isTrafficRelated(sf: SimpleFeature) = {
      val text = sf.get[String]("text").toLowerCase
      terms.find(text.contains).nonEmpty
    }

    def getCounty(sf: SimpleFeature): Option[String] = {
      countyMap.find{ case (c, g) => g.contains(sf.point) }.map{ case (c, g) => c}
    }


    val countyAndTweet = queryRDD.mapPartitions { iter =>
      iter.flatMap { sf =>
        getCounty(sf).map { county =>
          (county, isTrafficRelated(sf))
        }
      }
    }

    countyAndTweet
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
      .saveAsTextFile(outdir)
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
