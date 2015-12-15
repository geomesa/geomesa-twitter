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

object StalkerFinder {
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
      org.locationtech.geomesa.utils.geohash.GeoHash.apply(sf.geometry.getCentroid, Args.ghBits)
    }

    val userAndGeoHash = queryRDD.mapPartitions { iter =>
      val dtf = ISODateTimeFormat.basicDate()
      iter.map { sf =>
        val gh = geoHash(sf)
        val user = sf.get[String]("user_name")
        val day = dtf.print(sf.get[Date]("dtg").getTime)
        (user, day, gh)
      }
    }

    // create pairs of ( (user1, user2), (day, geohash) ) for when users are in the
    // same place at the same time
    val sameDaySameTimePairs = userAndGeoHash
      .groupBy { case (u, d, g)  => (d, g) }
      .flatMap { case ( (d, g), iter) =>
        val users = iter.map { case (user, day, geohash) => user }.toSet  //need a set bc of mutiple observations...
        val pairs = users.toStream.combinations(2).map{ case seq => (seq(0), seq(1))}   //convert back to 2 tuple
        pairs.map{ case (x:String, y: String) => ( (x, y), (d, g))}
      }

    // group by day/time,
    // filter by min number of times 2 users were in the same place at the same time,
    // and print
    sameDaySameTimePairs
     .groupBy(_._1)
     .filter(_._2.size > Args.minCount)
     .map { case ((x, y), iter) => s"($x, $y)" + "\t" + iter.map{ case ((_, _), (u, g)) => s"($u, $g)"}.mkString(", ") }
     .foreach(println)

  }

  object Args extends FeatureParams {
    @Parameter(names = Array("--output-dir"), description = "hdfs output dir for tsv", required = true)
    var outputDir: String = null

    @Parameter(names = Array("--min-count"), description = "min num counts", required = true)
    var minCount: Integer = 0

    @Parameter(names = Array("--geo-hash-bits"), description = "geo hash bits (try 25?)", required = true)
    var ghBits: Integer = 25
  }

}
