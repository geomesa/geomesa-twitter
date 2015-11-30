package geomesa.example.twitter.storm

import java.io.{BufferedReader, InputStreamReader}
import java.util
import java.util.UUID
import java.util.zip.GZIPInputStream

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.{IRichSpout, OutputFieldsDeclarer}
import backtype.storm.tuple.Fields
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class HdfsSpout extends IRichSpout {
  var fileList: java.util.List[String] = null
  var pos: Integer = 0
  var _reader: BufferedReader = null
  var spoutOutputCollector: SpoutOutputCollector = null
  var conf: Configuration = null
  override def getComponentConfiguration: util.Map[String, AnyRef] = {
    new util.HashMap[String, AnyRef]()
  }

  override def declareOutputFields(outputFieldsDeclarer: OutputFieldsDeclarer): Unit = {
    outputFieldsDeclarer.declare(new Fields("str"))
  }

  override def nextTuple(): Unit =
    Option(getReader()).map { reader =>
      var line = reader.readLine()
      while (line != null) {
        val id = UUID.randomUUID().toString.asInstanceOf[AnyRef]
        spoutOutputCollector.emit(util.Arrays.asList(line).asInstanceOf[java.util.List[AnyRef]], id)
        line = reader.readLine()
      }
      reader.close()
    }

  private def getReader() = {
    if (_reader == null && pos < fileList.size()) {
      val f = fileList.get(pos)
      pos += 1

      val fs = FileSystem.get(conf)
      val path = new Path(s"hdfs://$f")
      new BufferedReader(new InputStreamReader(new GZIPInputStream(fs.open(path))))
    } else null
  }

  override def activate(): Unit = {
    fileList = IOUtils.readLines(getClass.getClassLoader.getResourceAsStream("files"))
    pos = 0

  }

  override def deactivate(): Unit = {}

  override def close(): Unit = {}

  override def fail(o: scala.Any): Unit = {}

  override def open(map: util.Map[_, _], topologyContext: TopologyContext, spoutOutputCollector: SpoutOutputCollector): Unit = {
    this.spoutOutputCollector = spoutOutputCollector

    conf = new Configuration()
    conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    conf.addResource(new Path("/opt/hadoop/conf/core-site.xml"));
  }

  override def ack(o: scala.Any): Unit = {}
}
