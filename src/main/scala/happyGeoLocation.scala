/**
  * Created by arnaudstiegler on 27/03/2018.
  */

package sentimentScore.spark.streaming.twitter.example

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import twitter4j.Status
import org.elasticsearch.spark._


object happyGeoLocation extends App {

  // You can find all functions used to process the stream in the
  // Utils.scala source file, whose contents we import here
  import Utils._

  // First, let's configure Spark
  // We have to at least set an application name and master
  // If no master is given as part of the configuration we
  // will set it to be a local deployment running an
  // executor per thread

  val sparkConfiguration = new SparkConf().setMaster("local[1]")
    .setAppName("StreamingExample")
    .set("es.index.auto.create", "true")
    .set("es.nodes", "localhost:9200")

  // Let's create the Spark Context using the configuration we just created
  val sparkContext = new SparkContext(sparkConfiguration)

  // Now let's wrap the context in a streaming one, passing along the window size
  val streamingContext = new StreamingContext(sparkContext, Seconds(5))


  // Creating a stream from Twitter (see the README to learn how to
  // provide a configuration to make this work - you'll basically
  // need a set of Twitter API keys)
  val tweets: DStream[Status] = TwitterUtils.createStream(streamingContext, None)
/*
  tweets.map(t => List(t.getUser().getName(), t.getText(), t.getCreatedAt(), List(t.getGeoLocation().getLongitude(), t.getGeoLocation().getLatitude())))
    .foreachRDD(tweets => {
      tweets.collect().foreach(println)
    });

  */

  tweets.map(t => (t.getText.toString,t.getGeoLocation)).foreachRDD(t=>{
    System.out.print("map is done")
    //t.collect.foreach(System.out.print(t))
  })

  /*
  tweets.filter(t => t.getGeoLocation() != null)
        .map(t => List(t.getUser().getName(), t.getText(), t.getCreatedAt(), List(t.getGeoLocation().getLongitude(), t.getGeoLocation().getLatitude())))
        .foreachRDD(tweets => {
          tweets.collect().foreach(println)
        });
*/
  // Now that the streaming is defined, start it
  streamingContext.start()

  // Let's await the stream to end - forever
  streamingContext.awaitTermination()

}