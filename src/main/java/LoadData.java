
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;

import com.google.common.io.Files;

public class LoadData {
public static Long load() throws InterruptedException, IOException {
    String jsonPath = "C:\\Users\\Rahul\\Desktop\\GCP\\Key\\My First Project-5bea8475ed1c.json";
    GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath));

    BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();
    String datasetName = "SampleAvro";
    String tableName="Sample";
    String location = "asia-south1";
    File filePath = new File("C:\\Users\\Rahul\\IdeaProjects\\GCP-Ingestion\\src\\main\\resources\\SampleFiles\\userdata1.avro");
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
}
