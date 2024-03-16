package volt.dataloader;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.col;

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
            .config("spark.es.nodes", "localhost")
            .config("spark.es.port", "9200")
            .config("spark.es.net.http.auth.user", "elastic")
            .config("spark.es.net.http.auth.pass", "changeme")
            .config("spark.es.net.ssl", "true")
            .config("es.net.ssl.cert.allow.self.signed", "true")
            .config("spark.es.net.ssl.truststore.location", "file:///home/rsandor/repos/Volt/conf/volt_store.jks") // Path
            .config("spark.es.net.ssl.truststore.password", "changeme") // Path
            .appName("LoadData")
            .getOrCreate()) {

      var cves = spark.read().option("multiline", "true").json("data/nvdcve-1.1-2020.json");

      var exploded = cves.select(explode(col("CVE_Items")).as("cves"));

      Dataset<Row> exploded2 = exploded.selectExpr("cves.*");
      exploded2.printSchema();
      Dataset<Row> ds = exploded2.selectExpr("cve.description.description_data.value as cve_description",
          "cve.problemtype.problemtype_data.description as problem_description",
          "impact.baseMetricV3.cvssV3.*",
          "impact.baseMetricV3.exploitabilityScore",
          "impact.baseMetricV3.impactScore",
          "configurations",
          "lastModifiedDate",
          "publishedDate");

      ds.printSchema();

      ds.write().format("org.elasticsearch.spark.sql")
          .mode(SaveMode.Overwrite).save("cveindex");

    } catch (Exception ex) {
      ex.printStackTrace();
    }

  }
}
