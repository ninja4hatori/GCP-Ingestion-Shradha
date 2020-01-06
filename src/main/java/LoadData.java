
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;

import java.io.*;
import java.nio.channels.Channels;
import java.util.ArrayList;

import com.google.common.io.Files;

import org.apache.spark.sql.*;

public class LoadData {
    public static String jsonPath = "C:\\Users\\Rahul\\Desktop\\GCP\\Key\\My First Project-5bea8475ed1c.json";
    public static String datasetName = "SampleAvro";
    public static String tableName="Sample";
    public static String location = "asia-south1";
    public static String fPath = "C:\\Users\\Rahul\\IdeaProjects\\GCP-Ingestion\\src\\main\\resources\\SampleFiles\\userdata1.avro";

    public static Long load() throws InterruptedException, IOException {
    GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath));
    BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    File filePath = new File(fPath);
    TableId tableId = TableId.of(datasetName, tableName);
    WriteChannelConfiguration writeChannelConfiguration =
            WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.avro()).build();

// The location must be specified; other fields can be auto-detected.
    JobId jobId = JobId.newBuilder().setLocation(location).build();
    TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);
// Write data to writer
    try (OutputStream stream = Channels.newOutputStream(writer)) {
        Files.copy(filePath, stream);
    }
// Get load job
    Job job = writer.getJob();
    job = job.waitFor();
    JobStatistics.LoadStatistics stats = job.getStatistics();
    return stats.getOutputRows();

}

public static void query () throws IOException, InterruptedException {
    GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath));

    BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    String query = "SELECT id, first_name, last_name FROM `" + datasetName + "." + tableName +"` where country = \"Indonesia\";";
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();
    //DataFrame df;
// Print the results.
    for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
        //df = row.toArray();
        int colNumber=0;
        for (FieldValue val : row) {
           // System.out.printf("%s,", val.toString());
            switch (colNumber){
                case 0:
                    System.out.println("id:" + val.getStringValue());
                    break;
                case 1:
                    System.out.println("first_name:" + val.getStringValue());
                    break;
                case 2:
                    System.out.println("last_name:" + val.getStringValue());
                    break;
            }
            colNumber++;
        }
        System.out.printf("\n");
    }
}
}

