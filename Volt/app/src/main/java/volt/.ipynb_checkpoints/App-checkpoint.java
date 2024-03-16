package volt;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.sum;

import io.javalin.Javalin;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;

public class App {

  final static String parquetFile = "data/tfidf.parquet";

  public static void main(String args[]) {
    processCVEData();
    var app = Javalin.create(/* config */)
        .get("/query", ctx -> {
          final Object[] keywords =  ctx.queryParam("query").split(",");
          // Filter matching documents
          try {
            final var spark = SparkSession
              .builder()
              .master("local[*]")
              .config("spark.driver.host", "127.0.0.1")
              .config("spark.driver.bindAddress", "127.0.0.1")
              .config("spark.sql.shuffle.partitions", 8)
              .config("spark.default.parallelism", 8)
              .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
              .appName("volt")
              .getOrCreate();

            Dataset<Row> weightedWordDocuments = spark.read().parquet(parquetFile);

            Dataset<Row> matchingDocs = weightedWordDocuments
            .where(col("token").isin(keywords));

            matchingDocs.show();

            // Group by specified columns and aggregate
            Dataset<Row> rankedDocs = matchingDocs
            .groupBy("id")
            .agg(sum("tf_idf").alias("docrankcolumn"))
            .orderBy(col("docrankcolumn").desc());
            rankedDocs.show();

            ctx.json(rankedDocs.toJSON().collectAsList());

            spark.stop();
          } catch (Exception e) { 
            e.printStackTrace(); 
          }
        })
        .start(7070);
    
  }

  public static void processCVEData() {

    try (
        // Create the Spark Session
        SparkSession spark = SparkSession
            .builder()
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", 8)
            .config("spark.default.parallelism", 8)
            .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", true)
            .appName("volt")
            .getOrCreate()) {
      /*
       * Dataset is an extension of DataFrame, thus we can consider a DataFrame an
       * untyped view of a dataset. A Dataset is a strongly typed collection of
       * domain-specific objects that can be transformed in parallel using functional
       * or relational operations. Each Dataset also has an untyped view called a
       * DataFrame, which is a Dataset of Row.
       * See:
       * https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/Dataset.
       * html
       */
      Dataset<Row> cves = spark.read().option("multiline", "true").json("data/nvdcve-1.1-2020.json");

      /**
       * When you apply transformations like explode to a DataFrame,
       * Spark processes the data in parallel across multiple partitions (chunks of
       * data).
       */
      var exploded = cves.select(explode(col("CVE_Items")).as("cves"));

      // TODO: For now I only care about three pieces of information from the Dataset.
      // The cve-id, the description and parsing the the CPE that contains the
      // vulnerability type.

      // TODO: Create DFs for description, and CPE adding the cve-ID.
      var descrDS = exploded.select(col("cves.cve.CVE_data_meta.ID").as("id"),
          col("cves.cve.description.description_data.value").as("description"));

      long docCount = descrDS.select(countDistinct("id")).first().getLong(0);

      // Concatenates the array of strings into a single string.
      // The data is coming back as a array of strings, even if the array only has one
      // element. Same as using an RDD map function and providing the function.
      descrDS = descrDS.withColumn("description_single",
          functions.concat_ws(" ", descrDS.col("description")));

      descrDS = tokenize(descrDS);

      var tokensDS = descrDS.selectExpr("id", "explode(tokens) as token");
      var tfIdfDS = tfIdf(tokensDS, docCount, spark);

      final String codec = "parquet";

      tfIdfDS.write().format(codec).save(parquetFile);

    } catch (Exception ex) {
      ex.printStackTrace();
    }

  }

  /**
   * Tokenization of the decription field.
   * 
   * There are some considerations when tokenizing this data set.
   * For example, vulnerabilities often contain software packages
   * explicitily stated. Many of these packages contain special characters
   * such my_lib.so or parent.package.app.java. which are important to the meaning
   * of the cve and thus need to be retained when removing special characters.
   *
   * @param descrDS description Dataset containing just a single string
   *                description to tokenize.
   */
  private static Dataset<Row> tokenize(Dataset<Row> descrDS) {

    // case folding apply to the entire string first (more efficient than post
    // tokenization)
    var cleanTokens = descrDS.map(
        (MapFunction<Row, Row>) row -> {
          String[] descrTokens = stringCleaner(row.<String>getAs("description_single")).split(" ");
          return RowFactory.create(row.getAs("id"), (Object) descrTokens);
        },
        Encoders.row(new StructType()
            .add("id", DataTypes.StringType)
            .add("lower", DataTypes.createArrayType(DataTypes.StringType))));

    // Add the lower cased description to our descriptionDS.
    // Same thing as rdd matching on keys.
    descrDS = descrDS.join(cleanTokens, "id");

    descrDS = new StopWordsRemover().setInputCol("lower").setOutputCol("tokens").transform(descrDS);

    // Filter out rows with empty token arrays
    descrDS = descrDS.filter((FilterFunction<Row>) row -> {
      List<String> tokens = row.getList(row.fieldIndex("tokens"));
      return tokens != null && !tokens.isEmpty();
    });

    return descrDS;

  }

  public static String stringCleaner(String input) {
    // 1. Replace all "." or ':' followed by whitespace with an empty string.
    // a. remove ending periods.
    // 2. remove trademark, rights.
    // 3. grab content in parens only.
    // 4. remove some punction.
    return input.replaceAll("[.:\\,]+\\s+|\\.$|\\'|\\(TM\\)|\\(R\\)|\\(|\\)|\\\"", " ").strip().toLowerCase();
  }

  private static Dataset<Row> tfIdf(Dataset<Row> tokens, long docCount, SparkSession spark) {

    var tfDS = tokens.groupBy("id", "token").agg(count("id").as("tf"));
    var dfDS = tokens.groupBy("token").agg(countDistinct("id").as("df"));
    System.out.println(docCount);

    // Define the UDF for IDF calculation
    UDFRegistration udfRegistration = spark.udf();
    udfRegistration.register("calcIdfUdf", (UDF1<Long, Double>) df -> calcIdf(docCount, df), DataTypes.DoubleType);

    // Calculate IDF and add it as a new column "idf"
    var tokensWithIdf = dfDS.withColumn("idf", callUDF("calcIdfUdf", col("df")));

    // Show the resulting DataFrame
    tokensWithIdf.show();
    Dataset<Row> tfIdfDS = tokensWithIdf.join(tfDS, "token", "left")
        .withColumn("tf_idf", col("tf").multiply(col("idf")));

    return tfIdfDS;
  }

  private static double calcIdf(long docCount, long df) {
    // Calculate the TF-IDF using natural log.
    return Math.log((docCount + 1.0) / (df + 1.0));
  }
}
