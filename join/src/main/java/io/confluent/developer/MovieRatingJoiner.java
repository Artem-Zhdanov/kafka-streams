package io.confluent.developer;

import org.apache.kafka.streams.kstream.ValueJoiner;

// import io.confluent.developer.avro.Movie;
// import io.confluent.developer.avro.RatedMovie;
// import io.confluent.developer.avro.Rating;

public class MovieRatingJoiner implements ValueJoiner<String, String, String> {

  public String apply(String sVal , String tVal) {
    return sVal + "@@@" + tVal;
  }
}

