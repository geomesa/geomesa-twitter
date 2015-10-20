package geomesa.example.twitter.storm

import backtype.storm.topology.TopologyBuilder
import backtype.storm.{Config, StormSubmitter}
import com.beust.jcommander.{JCommander, Parameter, ParameterException}
import com.typesafe.scalalogging.slf4j.Logging
import geomesa.example.twitter.ingest.{Runner, TwitterFeatureIngester}
import geomesa.example.twitter.storm.TwitterStormIngest.StormArgs
import org.geotools.data.DataStoreFinder
import storm.kafka.{KafkaSpout, SpoutConfig, ZkHosts}

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

    val ds = DataStoreFinder.getDataStore(params)
    val sft = TwitterFeatureIngester.buildExtended(args.featureName)
    ds.createSchema(sft)

    val builder = new TopologyBuilder
    val spoutConfig = new SpoutConfig(new ZkHosts(args.brokers), args.topic, "/twitter-kafka-spout", "storm-consumer")
    builder.setSpout("k", new KafkaSpout(spoutConfig), 1)
    builder.setBolt("g", new GeoMesaIngestBolt(), 10).localOrShuffleGrouping("k")

    val conf = new Config()
    conf.setDebug(true)
    conf.setNumWorkers(10)
    conf.setMaxSpoutPending(1000)
    conf.setNumAckers(2)
    conf.setMaxTaskParallelism(20)
    conf.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 16384: Integer)
    conf.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, 16384: Integer)
    conf.put(Config.TOPOLOGY_RECEIVER_BUFFER_SIZE, 8: Integer)
    conf.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, 32: Integer)
    conf.put(Config.TOPOLOGY_STATS_SAMPLE_RATE, 0.05: java.lang.Double)
    params.foreach { case (k,v) => conf.put(s"${TwitterStormIngest.base}.$k", v.asInstanceOf[String]) }

    conf.put(TwitterStormIngest.FeatureName, args.featureName)

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

  class StormArgs extends Runner.GeoMesaFeatureArgs {
    @Parameter(names = Array("--brokers", "-b"), description = "Kafka brokers", required = true)
    var brokers: String = null

    @Parameter(names = Array("--topic", "-t"), description = "Kafka brokers", required = true)
    var topic: String = null
  }

  def main(args: Array[String]): Unit = {
    //parse args
    val clArgs: StormArgs = new StormArgs
    try {
      new JCommander(clArgs, args: _*)
      new TwitterStormIngest(clArgs).run()
    }
    catch {
      case e: ParameterException => {
        logger.info("Error parsing arguments: " + e.getMessage, e)
        System.exit(-1)
      }
    }
  }

}
