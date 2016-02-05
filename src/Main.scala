/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//works for now
//
// scalastyle:off println
package org.apache.spark.examples.streaming
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.{StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import com.google.gson

/**
  * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
  * stream. The stream is instantiated with credentials and optionally filters supplied by the
  * command line arguments.
  *
  * Run this on your local machine as
  *
  */
object Main {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    System.out.println ("does this work" +consumerKey);

    val auth = Array(consumerKey, consumerSecret, accessToken, accessTokenSecret)
    val sparkConf = new SparkConf().setAppName("Main")
                                   .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val jssc = new JavaStreamingContext(sparkConf,Seconds(2))

    val stream = TwitterUtils.createStream(ssc, None, filters)
    //val tweetStream = TwitterUtils.createStream(jssc, auth)
    val time = new Time(1000)
    //tweetStream.compute(time)
    //val test = tweetStream.receiverInputDStream.start()

    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("")))
    val hashTags2 = stream.flatMap(status => status.getUser.getDescription.split(" ").filter(_.startsWith("engineer")));
    //val hashTags3 = stream.flatMap(status => status.getUser.getDescription)


   val names = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (hi, count) => (count, hi)}
      .transform(_.sortByKey(false))

    //val names2 = hashTags2.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
     // .map{case (topic, count) => (count, topic)}
     // .transform(_.sortByKey(false))

   // val topCounts102 = hashTags2.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
     //.map{case (topic, count) => (count, topic)}
      //.transform(_.sortByKey(false))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{case (hi, count) => (count, hi)}
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (hi, count) => (count, hi)}
      .transform(_.sortByKey(false))

    //names2.foreachRDD(rdd => {
      //val topList = rdd.take(10)
      //println("\nPopular ATS @@@@ in last 10 seconds (%s total):".format(rdd.count()))
     // topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    //})
    names.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    // Print popular hashtags
   topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
     println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
     println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println