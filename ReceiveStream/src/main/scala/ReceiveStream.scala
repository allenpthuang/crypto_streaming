import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SparkSession

// import NLP libraries from John Snow Labs
import com.johnsnowlabs.nlp.base.DocumentAssembler
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.Normalizer
import com.johnsnowlabs.nlp.annotators.sda.vivekn.ViveknSentimentModel
import com.johnsnowlabs.nlp.Finisher
import org.apache.spark.ml.Pipeline

import java.time.Instant


object ReceiveStream {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: ReceiveStream <host> <port>")
      System.exit(1)
    }

    val HOST = args(0)
    val PORT = args(1).toInt

    val INTEVAL = 300

    val conf = new SparkConf().setMaster("local[2]")
                              .setAppName("ReceiveStream")
    //val sc = new SparkContext(conf)
    val ssc = new StreamingContext(conf, Seconds(INTEVAL))
    val spark = SparkSession.builder()
                            .master("local[2]")
                            .appName("ReceiveStream")
                            .getOrCreate()
    import spark.implicits._

    // Build the NLP pipeline
    val document = new DocumentAssembler()
      .setInputCol("message")
      .setOutputCol("document")

    val token = new Tokenizer()
      .setInputCols("document")
      .setOutputCol("token")

    val normalizer = new Normalizer()
      .setInputCols("token")
      .setOutputCol("normal")

    val vivekn = ViveknSentimentModel.pretrained()
      .setInputCols("document", "normal")
      .setOutputCol("result_sentiment")

    val finisher = new Finisher()
      .setInputCols("result_sentiment")
      .setOutputCols("final_sentiment")

    val pipeline = new Pipeline().setStages(Array(document, token, normalizer, vivekn, finisher))


    // Create a socketTextStream to receive data
    val jsonStream = ssc.socketTextStream(HOST, PORT)
    
    // Process each RDD and read as JSON strings
    // Pipe them into the NLP pipeline and show the results
    var unixTimestamp = Instant.now.getEpochSecond
    jsonStream.foreachRDD(rdd => {
      unixTimestamp = Instant.now.getEpochSecond
      if (! rdd.isEmpty) {
        val df = spark.read.json(rdd)
        df.show()

        val pipelineModel = pipeline.fit(df)
        val result = pipelineModel.transform(df)
        result.select("final_sentiment").show(true)
        val sentimentCnt = result.groupBy("final_sentiment").count()
        sentimentCnt.show()

        val pos = sentimentCnt.filter(sentimentCnt("final_sentiment") === Array("positive"))
                              .select("count").collect()
        val na = sentimentCnt.filter(sentimentCnt("final_sentiment") === Array("na"))
                             .select("count").collect()
        val neg = sentimentCnt.filter(sentimentCnt("final_sentiment") === Array("negative"))
                              .select("count").collect()

        var posCnt = 0.0
        var naCnt = 0.0
        var negCnt = 0.0
        if (pos.size != 0) {
          posCnt = pos(0)(0).toString.toFloat
        }      
        if (na.size != 0) {
          naCnt = na(0)(0).toString.toFloat
        }
        if (neg.size != 0) {
          negCnt = neg(0)(0).toString.toFloat
        }

        val score = (posCnt + 0.5 * naCnt) / (posCnt + naCnt + negCnt)
        val time = unixTimestamp
        print("\n============\n" + time + "\n"
              + "score = " +  score + "\n"
              + "  posCnt = " + posCnt + "\n"
              + "  negCnt = " + negCnt + "\n"
              + "  naCnt = " + naCnt + "\n"
              + "============\n\n")
        val scoreStr = time + "," + score
        val dfScore = Seq((time, score, posCnt, negCnt, naCnt))
                        .toDF("report_time", "score", "posCnt", "negCnt", "naCnt")

        dfScore.repartition(1).write.mode("append").csv("report.csv")
      } else {
        println("empty rdd!")
      }
    })

    ssc.start()
    ssc.awaitTermination()

    //sc.stop()  // optional
  }
}
