package geomesa.example.twitter.spark

import java.io.{InputStream, FileInputStream}

import org.apache.commons.io.IOUtils

import scala.collection.JavaConversions._

class TwitterSentiment {
//  val happyMap = scala.collection.mutable.Map.empty[String, Double]
//  val sadMap = scala.collection.mutable.Map.empty[String, Double]
  val scoreMap = scala.collection.mutable.Map.empty[String, Int]

  def loadFile(f: String) = {
    val is = new FileInputStream(f)
    try {
      load(is)
    } finally {
      is.close()
    }
  }

  def load(is: InputStream): Unit = {
    val lines = IOUtils.readLines(is)
    lines.foreach { line =>
      val s = line.split("\t")
      scoreMap.put(s(0), s(1).toInt)
    }
//    lines.drop(1).foreach { line =>
//      val s = line.split(",")
//      happyMap.put(s(0).toLowerCase, s(1).toDouble)
//      sadMap.put(s(0).toLowerCase(), s(2).toDouble)
//    }
  }

  // returns probability of happy
  def classify(words: List[String]): Double = {
//    val happyLogScore = happyMap.filterKeys(words.contains).values.sum
//    val sadLogScore = sadMap.filterKeys(words.contains).values.sum
//    1.0 / ( (sadLogScore - happyLogScore) + 1)
    words.map(scoreMap.getOrElse(_, 0)).sum.toDouble
  }

}

object TwitterSentiment {
  def main(args: Array[String]): Unit = {
    val ts = new TwitterSentiment()
    ts.loadFile("/home/ahulbert/twitter-sentiment/twittwordlist/twitter_sentiment_list.csv")
    println(ts.classify(List("very", "sad", "traffic")))
  }
}
