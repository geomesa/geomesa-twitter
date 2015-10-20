package geomesa.example.twitter.storm

import java.util

import backtype.storm.task.{OutputCollector, TopologyContext}
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichBolt
import backtype.storm.tuple.Tuple
import geomesa.example.twitter.ingest.{TwitterFeatureIngester, TwitterParser}
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes

class TwitterParserBolt extends BaseRichBolt {

  var parser: TwitterParser = null

  override def execute(tuple: Tuple): Unit = {

  }

  override def prepare(map: util.Map[_, _], topologyContext: TopologyContext, outputCollector: OutputCollector): Unit = {
    val featureName = map.get(TwitterStormIngest.FeatureName).asInstanceOf[String]
    val sft = TwitterFeatureIngester.buildExtended(featureName)
    parser = new TwitterParser(featureName, sft, true)




  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    //TODO twitter here
  }
}
