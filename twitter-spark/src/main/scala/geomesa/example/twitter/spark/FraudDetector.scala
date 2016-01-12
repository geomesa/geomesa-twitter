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
    // NB: This map is serializable, so we can pass it between Spark workers.
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
    // This call sets up multiple configurations including information regarding SimpleFeature serialization.
    val sconf = GeoMesaSpark.init(new SparkConf(true), ds)  
    val sc = new SparkContext(sconf)

    import org.locationtech.geomesa.utils.geotools.Conversions._
    // Create an RDD from a query
    val queryRDD = org.locationtech.geomesa.compute.spark.GeoMesaSpark.rdd(conf, sc, params, q)

    def geoHash(sf: SimpleFeature) = {
      org.locationtech.geomesa.utils.geohash.GeoHash.apply(sf.geometry.getCentroid, 25)
    }

    // This computes an RDD[String, String, String] which holds the (Username, Day, GeoHash).  
    // The day is written in the ISO Date format.  The GeoHash is a 5-character / 25-bit GeoHash.
    // NB: No computation has occurred yet.
    val userAndGeoHash = queryRDD.mapPartitions { iter =>
      val dtf = ISODateTimeFormat.basicDate()
      iter.map { sf =>
        val gh = geoHash(sf)
        val user = sf.get[String]("user_name")
        val day = dtf.print(sf.get[Date]("dtg").getTime)
        (user, day, gh)
      }
    }

    // This block computes an RDD[ (User1, User2), (Day, GeoHash) ].
    // NB: No computation has occurred yet.
    val paired = userAndGeoHash
      .groupBy { case (u, d, g)  => (d, g) }
      .flatMap { case ( (d, g), iter) =>
        val peeps = iter.map(_._1)
        val combine =
          for {
              x <- peeps
              y <- peeps
          } yield (x, y)
        combine.map{ case (x, y) => ( (x, y), (d, g))}
      }

    // This block will for force evaluation due to its final .foreach{println}
    //  The groupBy will pull together pairs of people.  The filter requires that those pairs appear together at least Args.minCount times.
    //  The map transforms ( (user1, user2), (day, geohash), iter) into a like "(User1, User2)\t (day1, gh1), (day2, gh2)".
    paired
     .groupBy(_._1)
     .filter(_._2.size > Args.minCount)
     .map { case ((x, y), iter) => s"($x, y)" + "\t" + iter.map{ case ((_, _), (u, g)) => s"($u, $g)"}.mkString(", ") }
     .foreach(println)

  }

  object Args extends FeatureParams {
    @Parameter(names = Array("--output-dir"), description = "hdfs output dir for tsv", required = true)
    var outputDir: String = null

    @Parameter(names = Array("--min-count"), description = "min num counts", required = true)
    var minCount: Integer = 0
  }

}
