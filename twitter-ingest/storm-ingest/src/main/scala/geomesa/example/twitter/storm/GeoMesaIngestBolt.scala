package geomesa.example.twitter.storm

import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import geomesa.example.twitter.ingest.{TwitterFeatureIngester, TwitterParser}
import org.apache.accumulo.core.conf.Property
import org.geotools.data.{Transaction, DataStoreFinder}
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.Z3Table

import scala.collection.JavaConversions._

class GeoMesaIngestBolt extends BaseRichBolt {

  private var ds: AccumuloDataStore = null
  private var parser: TwitterParser = null
  private var sfw: SFFeatureWriter = null
  private var outputCollector: OutputCollector = null
  private var skipIngest: Boolean = false

  override def execute(tuple: Tuple): Unit = {
    outputCollector.ack(tuple)
    val jsonMsg = tuple.getStringByField("str")
    if (! skipIngest) {
      val next = sfw.next()
      parser.parse(jsonMsg, next)
      sfw.write()
    }
  }

  override def prepare(map: util.Map[_, _], topologyContext: TopologyContext, outputCollector: OutputCollector): Unit = {
    val dsp = Map(
      "instanceId" -> map.get(TwitterStormIngest.InstanceId),
      "user" -> map.get(TwitterStormIngest.User),
      "password" -> map.get(TwitterStormIngest.Password),
      "tableName" -> map.get(TwitterStormIngest.TableName),
      "zookeepers" -> map.get(TwitterStormIngest.Zookeepers)
    )
    ds = DataStoreFinder.getDataStore(dsp).asInstanceOf[AccumuloDataStore]
    val featureName = map.get(TwitterStormIngest.FeatureName).asInstanceOf[String]
    sfw = ds.getFeatureWriterAppend(featureName, Transaction.AUTO_COMMIT)
    val sft = TwitterFeatureIngester.buildExtended(featureName)
    parser = new TwitterParser(featureName, sft, true)
    this.outputCollector = outputCollector
    this.skipIngest = map.get(TwitterStormIngest.SkipIngest).asInstanceOf[String].toBoolean

    GeomesaSystemProperties.BatchWriterProperties.WRITER_MEMORY_BYTES.set((1024l*1024*10).toString)
    GeomesaSystemProperties.BatchWriterProperties.WRITER_THREADS.set(20.toString)
    GeomesaSystemProperties.BatchWriterProperties.WRITER_LATENCY_MILLIS.set((1000l*120).toString)
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    // nothing
  }

  override def cleanup(): Unit = {
    sfw.close()
    ds = null
    sfw = null
    parser = null
    outputCollector = null
  }
}
