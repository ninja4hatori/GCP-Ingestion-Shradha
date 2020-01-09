import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.io.Files;

import java.io.*;
import java.nio.channels.Channels;

import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;

public class LoadData {
    public static GoogleCredentials credentials;
    public static BigQuery bigquery;

    //Method to connect to GCP via ServiceKey stored in JSON file.
    public static void connect(String jsonPath) throws IOException {
        credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath));
        bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    }

    // Method to load a file from local to a BigQuery Table.
    public static Long load(String datasetName, String tableName, String location, String fPath) throws InterruptedException, IOException {
    File filePath = new File(fPath);
    TableId tableId = TableId.of(datasetName, tableName);

    // Building WriteChannel. The location must be specified; Other fields can be auto-detected.
    WriteChannelConfiguration writeChannelConfiguration =
              WriteChannelConfiguration.newBuilder(tableId).setFormatOptions(FormatOptions.avro()).build();
    JobId jobId = JobId.newBuilder().setLocation(location).build();
    TableDataWriteChannel writer = bigquery.writer(jobId, writeChannelConfiguration);

    // Write data to writer
    try (OutputStream stream = Channels.newOutputStream(writer)) {
        Files.copy(filePath, stream);
    }
    // Get load job and return number of rows inserted from stats
    Job job = writer.getJob();
    job = job.waitFor();
    JobStatistics.LoadStatistics stats = job.getStatistics();
    return stats.getOutputRows();
}

// Method to query table and print the results.
public static void query (String datasetName, String tableName) throws InterruptedException {
    //Building QueryConfig
    String query = "SELECT id, first_name, last_name FROM `" + datasetName + "." + tableName +"` where country = \"Indonesia\";";
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).build();

    // Printing the results.
    for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
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

// Method to Query the data and save the results to local file via temp table
public static void saveToLocal(String datasetName, String tableName, String sPath, String jsonPath) throws InterruptedException {
    String destTableName = "temp";
    String fullDestTableName = datasetName + "." + destTableName;
    String base64 = "ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiZXZvY2F0aXZlLWxvZGdlLTI2MjQyMSIsCiAgInByaXZhdGVfa2V5X2lkIjogIjViZWE4NDc1ZWQxYzYwM2IxZjQxZmYwNjBkMDhkODI0OGIyMjAzYmMiLAogICJwcml2YXRlX2tleSI6ICItLS0tLUJFR0lOIFBSSVZBVEUgS0VZLS0tLS1cbk1JSUV2Z0lCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktnd2dnU2tBZ0VBQW9JQkFRREVlbmV1bFVpVG95ZUxcbjVwQWlhUnJreG5FOWZVM1g5NjE5cTNPdHZtVDlUdDFERm9obVFwZ013L3JyU3djRUdHSjBmaTI1cDFLdTVwd3FcbmNvKys0dVFvQWl5a1BnNCtiR0JIN3B6d0s3Zlo3SnFJRzlJZDl3M0VUbEg0WE0zOHllbW1uZFR0bVZQWXZHQWVcbnEzVGk3MDdCYnRaQVZnRlYyZE9hSWxZTHYzVGtVaGZkMEx5MFJ6aDZzYjN0cWtmSGY0NHpLaW5YU3Q5aEJiVlpcbjA3RmJHeGN4dlZJOWNnRXV1ZDlwSU5hR3VMZ2NwdnlBTXBrL3FGRDRKaUNSRWcrYXNycVh2Y2JHZ2RPdi9TeENcbmRCZStuYkQwMVh1dlYzeEEySUJMR1RiOG40STFyN3A1UzVDb0wrV3NXZVo1RG5yakxvMzNBMk9VNVpiZXVGaTRcbi9tSWFMZE9GQWdNQkFBRUNnZ0VBQVB4UFNPSXBVc3BFK3NQdGthcWV6cXA3SzNDTE9TdmVKQ3o5djgrZWNKSGFcbmRNbHlaamJyemRhVzFlTFN0bS9MNEtNNFpmL0RuNnA2K3BDSURaQlVUaW9Tb01GSjc0QzZLK296d2RXcVRxN0xcbjZLWmhFN0xuVUFhSGpUd2o4V0p1NkYzTzBXRk5SUjlPeVNDWmpjRGZENFBYa2N1TXBxcFkwQ05mZEtBaGZUcTBcbjhXMVFiNHZoRnhCSGpFZlVMQTZjYVlUaWRmWkFSQlN1b0FpLzhsSEVzOU9LTUhkRkFhRGI3a0U1UWM5amg0WUdcbnZtVHdnMFRjUFdXZkFmNU9zQTV6TDNJUGw3anJ6T3cyVWVPUURxcGVMMUpSYnhha1llOHU1WHhlL1ZWSkt4NHlcbmdwRFJ5VU1jM1NITXVYOUsyaWgwdzgzcFJFSENQMGFoY3psaWxjaGR1UUtCZ1FEZ3FET3hYRUJLNDhkdElPY2JcbmxJaTY4Umg4RE5Da1ZMRGs1QlUrcnBxaU1vcjJQQllUZHlYVWV2OGtxSnNhTmVMRm9rYTJhTlg5NVlEYzlSM0tcbnIrZDlCMnVqcmcwYUhwckMxMWkxUzJwdUxNbGRSSXVSQUYzYkM1QlphOXNtVkxwUUpLTTgySkxwb01LdXNKaWNcbnFwVmx6N0RhUkc5bmJ0amk4WWk5M2ZvZHVRS0JnUURmNDlmeGozV1h5Qk5PT09kclhQMFVMY2xHVjZYYkFqK2dcbi9Dc1h0TFNkQnN1T3o3K2Y3UVpGb2tnN1NIMkYxR3htMGJWeTdxdkZpWUcyWVJ4dElHcmh3ZmRqVUI5d1M5Lzdcbjdta2FQZXJEVUxWSjV5TEVjNkM5UWFtWElNN2V4c2dta3JLUTREMU0xeGhtZUF1eEdEeU0vUERBM0dvY3VKVWNcbnV1OFM0Q2hxTFFLQmdRQ2lhTGJHRjF6YlJ3UGEzcGgwbGRLcTRyVENxVGtFTTcyV0cxVklkVTJReUYweFdZclVcbkU2U2prUzkyUHZXeDR4YkhyV0xWWjhDYnhoaDhwQzhmWWo5RlllSHMrRnk4YnplT2Q4UEhmSGU2b21JSUxRK0FcbmlmVlA3M0l4VXdtaEVrdUd1SlhSM1BlSU1oSEwzQnJYMTNJZG9pSEdDUWRJalJmNktJYWtUQjhPOFFLQmdRQ3Vcbk44alFzclpwbm9uUFE1NW93Qm82K29uMXo3eEMyTlFVZkVVNEZDaTdUQTlZR0xiZlJueXI4T1RPSk5Gd00yVUtcbitVSjlwZFZLU0g4RUlUc1NlN2hQNWpTUU5rZlFoV3BNeXk5RCtVeFdJZGFBSkhpOGI5RnprOFhZMFBISkR0dXVcbmtGYWRQN0RUdTBqRWE4T0ZVZnZFSmd0ZHQrWm1aUWU3TElkZW84a3ZIUUtCZ0FFQW0rayt0UlNpcDZVRDNsUXNcbitlRUYyN1lEVm9mV0RJd0FIa0kvUHdUWXhadnpORXNzTWROc0IyV2RkbG5xZ3NZUThDZ1hwSUZzVW03Z2NCODJcbndCQ3l0QkpWTVlkcDhGaW1ReUtic2Q4czlEUm95S0V0ODk0ekRYZXY4UGUrOUJITzFrUXVZUmRBRjNPS0NuN25cbnN6dERHcC9jV082MDBtdnovNTlSaFFFOFxuLS0tLS1FTkQgUFJJVkFURSBLRVktLS0tLVxuIiwKICAiY2xpZW50X2VtYWlsIjogInNocmFkc0Bldm9jYXRpdmUtbG9kZ2UtMjYyNDIxLmlhbS5nc2VydmljZWFjY291bnQuY29tIiwKICAiY2xpZW50X2lkIjogIjEwNDM1MTQxNzc1MTcyMjkyMzU5NSIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvc2hyYWRzJTQwZXZvY2F0aXZlLWxvZGdlLTI2MjQyMS5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIKfQo=";
    String query = "SELECT id, first_name, last_name FROM `" + datasetName + "." + tableName +"` where country = \"Indonesia\";";

    //Initializing a SparkSession
    System.setProperty("hadoop.home.dir", "c:\\winutil\\");
    SparkSession spark = SparkSession.builder().appName("SaveExtractFile").config("spark.master","local").config("spark.sql.avro.compression.codec","snappy").getOrCreate();

    //Running Query to save results in Destination Temp Table
    QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setDestinationTable(TableId.of(datasetName, destTableName)).build();
    bigquery.query(queryConfig);

    //Reading data from Destination Table and saving it to a local CSV file.
    Dataset<Row> ds = spark.read().format("bigquery").option("credentials", base64).option("table", fullDestTableName).load();
    System.out.println(ds.count());
    ds.coalesce(1)
            .write()
            .option("header","true")
            .option("sep",",")
            .mode("overwrite")
            .format("csv")
            .save(sPath);

    //Deleting the temp table after saving the extract file.
    bigquery.delete(TableId.of(datasetName, destTableName));
    spark.stop();
}
}

