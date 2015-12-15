package geomesa.example.twitter.storm

import java.util.UUID

import backtype.storm.spout.SchemeAsMultiScheme
import backtype.storm.topology.TopologyBuilder
import backtype.storm.{Config, StormSubmitter}
import com.beust.jcommander.{JCommander, Parameter, ParameterException}
import com.typesafe.scalalogging.slf4j.Logging
import geomesa.example.twitter.ingest.{Runner, TwitterFeatureIngester}
import geomesa.example.twitter.storm.TwitterStormIngest.StormArgs
import org.apache.accumulo.core.conf.Property
import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import storm.kafka.{KafkaSpout, SpoutConfig, StringScheme, ZkHosts}

import scala.collection.JavaConversions._

class TwitterStormIngest(args: StormArgs) extends Logging {

  def run(): Unit = {
    val params = Map(
      "instanceId" -> args.instanceId,
      "zookeepers" -> args.zookeepers,
      "user" -> args.user,
      "password" -> args.password,
      "tableName" -> args.catalog
    )

    val ds = DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
    val sft = TwitterFeatureIngester.buildExtended(args.featureName)
    ds.createSchema(sft)

    val table = Z3Table.formatTableName(args.catalog, sft)
    val tableOps = ds.connector.tableOperations
    tableOps.setProperty(table, Property.TABLE_SPLIT_THRESHOLD.getKey, "1G")
    tableOps.setProperty(table, Property.TABLE_SPLIT_THRESHOLD.getKey, "1G")
    tableOps.setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey, "true")

    val builder = new TopologyBuilder
    val spoutConfig = new SpoutConfig(new ZkHosts(args.zookeepers), args.topic, s"/${args.topic}", UUID.randomUUID().toString)
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme())
    spoutConfig.startOffsetTime = kafka.api.OffsetRequest.EarliestTime
    spoutConfig.forceFromStart = true
    builder.setSpout("kafka", new KafkaSpout(spoutConfig), args.numSpouts)
    builder.setBolt("geomesa", new GeoMesaIngestBolt(), args.numBolts).localOrShuffleGrouping("kafka")
//    builder.setSpout("hdfs", new HdfsSpout)
//    builder.setBolt("geomesa", new GeoMesaIngestBolt(), args.numBolts).localOrShuffleGrouping("hdfs")

    val conf = new Config()
    conf.setDebug(true)
    conf.setNumWorkers(args.numBolts + args.numSpouts)
    conf.setMaxSpoutPending(1000000)
    conf.setNumAckers(20)
    conf.setMaxTaskParallelism(40)
    params.foreach { case (k,v) => conf.put(s"${TwitterStormIngest.base}.$k", v.asInstanceOf[String]) }

    conf.put(TwitterStormIngest.FeatureName, args.featureName)
    conf.put(TwitterStormIngest.SkipIngest, args.skipIngest.toString)

    logger.info("Submitting topology")
    StormSubmitter.submitTopology("twitter", conf, builder.createTopology())
  }

}

object TwitterStormIngest extends Logging {
  val base = "geomesa.twitter.storm"
  val InstanceId = s"$base.instanceId"
  val Zookeepers = s"$base.zookeepers"
  val User = s"$base.user"
  val Password = s"$base.password"
  val TableName = s"$base.tableName"
  val KafkaBrokers = s"$base.kafkaBrokers"
  val FeatureName   = s"$base.featureName"
  val SkipIngest    = s"$base.skipIngest"

  class StormArgs extends Runner.GeoMesaFeatureArgs {
    @Parameter(names = Array("--brokers", "-b"), description = "Kafka brokers", required = true)
    var brokers: String = null

    @Parameter(names = Array("--topic", "-t"), description = "Kafka brokers", required = true)
    var topic: String = null

    @Parameter(names = Array("--bolts"), description = "number of bolts", required = true)
    var numBolts: Integer = 10

    @Parameter(names = Array("--spouts"), description = "number of spouts", required = true)
    var numSpouts: Integer = 5

    @Parameter(names = Array("--skip-ingest"), description = "skip ingest", required = true)
    var skipIngest: String = "false"
  }

  def main(args: Array[String]): Unit = {
    //parse args
    val clArgs: StormArgs = new StormArgs
    try {
      new JCommander(clArgs, args: _*)
      new TwitterStormIngest(clArgs).run()
    }
    catch {
      case e: ParameterException =>
        logger.info("Error parsing arguments: " + e.getMessage, e)
        System.exit(-1)
    }
  }

}
