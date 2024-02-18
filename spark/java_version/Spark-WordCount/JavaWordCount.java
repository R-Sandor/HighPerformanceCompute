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

package dev.findfirst.wordcount;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.Serializable;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

public final class JavaWordCount implements Serializable {
  private static Logger logger = LogManager.getLogger(JavaWordCount.class);

  public static class WordCountComparator implements Serializable, Comparator<Tuple2<String, Integer>> {
    @Override
    public int compare(Tuple2<String, Integer> left, Tuple2<String, Integer> right) {
        return left._2 - right._2;
    }
  }

  public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: JavaWordCount <file>");
      System.exit(1);
    }

    logger.debug("Starting spark session.");
    SparkSession spark = SparkSession
      .builder()
      .master("spark://hpd-master:7077")
      .appName("JavaWordCount")
      .getOrCreate();

    JavaRDD<String> inputFile = spark.read().textFile(args[0]).javaRDD();

    JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> content.toUpperCase()
			    //.replaceAll("[-+.^:,]","")
			    .split(" ")
			    .stream().map(s::replaceAll("[^A-Za-z0-9]","")).toList()
			    .iterator());
    var filtered = wordsFromFile.filter(word -> !word.strip().equals(""));
    JavaPairRDD<String, Integer> countData = filtered.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

    var maxTuple = countData.max(new WordCountComparator());
    JavaPairRDD<String, Integer> topWord = countData.filter(x -> x._1.equals(maxTuple._1));
    topWord.collect().forEach(line -> System.out.println(line));

    //topWord.saveAsTextFile(args[1]);

    spark.stop();
   }
}
