package geomesa.example.twitter.spark

import java.util.Date

import com.beust.jcommander.{JCommander, Parameter}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.geotools.data.{DataStoreFinder, Query}
import org.joda.time.format.ISODateTimeFormat
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.compute.spark.GeoMesaSpark
import org.locationtech.geomesa.tools.commands.FeatureParams
import org.opengis.feature.simple.SimpleFeature

object FraudDetector {
  def main(args: Array[String]): Unit = {
    new JCommander(Args, args.toArray: _*)

    import scala.collection.JavaConversions._

    // Get a handle to the data store
    val params = Map(
      "instanceId" -> Args.instance,
      "zookeepers" -> Args.zookeepers,
      "user"       -> Args.user,
      "password"   -> Args.password,
      "tableName"  -> Args.catalog)

    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
    val q = new Query(Args.featureName)

    // Configure Spark
    val conf = new Configuration
    val sconf = GeoMesaSpark.init(new SparkConf(true), ds)
    val sc = new SparkContext(sconf)

    import org.locationtech.geomesa.utils.geotools.Conversions._
    // Create an RDD from a query
    val queryRDD = org.locationtech.geomesa.compute.spark.GeoMesaSpark.rdd(conf, sc, params, q)

    def geoHash(sf: SimpleFeature) = {
      org.locationtech.geomesa.utils.geohash.GeoHash.apply(sf.geometry.getCentroid, 25)
    }

    val dtf = ISODateTimeFormat.basicDate()
    val userAndGeoHash = queryRDD.mapPartitions { iter =>
      iter.map { sf =>
        val gh = geoHash(sf)
        val user = sf.get[String]("user_name")
        val day = dtf.print(sf.get[Date]("dtg").getTime)
        (user, day, gh)
      }
    }

    // TODO find people who are interesting...

  }

    object Args extends FeatureParams {
    @Parameter(names = Array("--output-dir"), description = "hdfs output dir for tsv", required = true)
    var outputDir: String = null
  }

}
