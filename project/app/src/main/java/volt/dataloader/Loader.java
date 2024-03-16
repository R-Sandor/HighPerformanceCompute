package volt.dataloader;

import org.apache.spark.sql.SparkSession;

public class Loader {

  public static void main(String args[]) {
    processCVEData();
  }

  public static void processCVEData() {

    try (
        // Create the Spark Session
        SparkSession spark = SparkSession
            .builder()
            .master("local[3]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", 3)
            .config("spark.default.parallelism", 3)
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .appName("LoadData")
            .getOrCreate()) {

      var cves = spark.read().option("multiline", "true").json("data/nvdcve-1.1-2020.json");
      cves.printSchema();

    } catch (Exception ex) {
      ex.printStackTrace();
    }

  }
}