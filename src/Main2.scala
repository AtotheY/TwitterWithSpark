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

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.{StreamingContext}
import org.apache.spark.streaming.twitter.TwitterInputDStream
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import com.google.gson
import twitter4j.TwitterFactory
import twitter4j.auth.{OAuthAuthorization, Authorization}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import twitter4j.conf.ConfigurationBuilder
import twitter4j.internal.http.HttpRequest


/**
  * Calculates popular hashtags (topics) over sliding 10 and 60 second windows from a Twitter
  * stream. The stream is instantiated with credentials and optionally filters supplied by the
  * command line arguments.
  *
  * Run this on your local machine as
  *
  */

object Main2 {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //StreamingExamples.setStreamingLogLevels()

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 6)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    System.out.println ("does this work" +consumerKey);

    val config = new twitter4j.conf.ConfigurationBuilder()
      .setOAuthConsumerKey("2pzru6Cd8aRumYHUumoCiKwYS")
        .setOAuthConsumerSecret("rKKQfISDzk715OOKS7wvGkSKdkqGsLFOdTrZ9QdJOfAjMXekNB")
        .setOAuthAccessToken("962554652-SU0aBc8Iyukka1gFJQY9Ux5XczDXSgczRpuih3ml")
        .setOAuthAccessTokenSecret("k0DWDIGeJ6u2ijYJeIv6dfCuCmTPIZT8xPJ5AG4P9PV72")
        .build
    val twitter_auth = new TwitterFactory(config)
    val a = new OAuthAuthorization(config)
    val atwitter = twitter_auth.getInstance(a).getAuthorization()
    //val auth = new Option[Authorization] {}
    val auth = Option(atwitter)

    val sparkConf = new SparkConf().setAppName("Main2")
      .setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val test = new StorageLevel()

    val twitter = new TwitterInputDStream(ssc, auth, filters, test )
    val rec = twitter.getReceiver()
    twitter.print()

    System.out.println ( " TWITTER D STREAM " +twitter.toString)

    /*val filter = Array("@Anthony")
    val statuses = TwitterUtils.createStream(ssc, None, filter)
    val mapped = statuses.map(status => status.getUser.getScreenName)
    mapped.foreachRDD(t => {
      t.foreach(g=>{
        System.out.println ("USER : "+ g);
      })
    })
    statuses.print()*/
    ssc.start()
    ssc.awaitTermination()
  }
}
// scalastyle:on println