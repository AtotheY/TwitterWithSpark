import twitter4j.auth.AccessToken

/**
  * Created by AnthonyS on 12/19/2015.
  */
object Util {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }
    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    val config = new twitter4j.conf.ConfigurationBuilder()
      .setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessToken)
      .build
    val auth: twitter4j.auth.AccessToken = consumerKey.asInstanceOf[twitter4j.auth.AccessToken]
    val acc: twitter4j.auth.Authorization = Array(consumerKey,consumerSecret,accessToken,accessTokenSecret).asInstanceOf[twitter4j.auth.Authorization]

  }

}
