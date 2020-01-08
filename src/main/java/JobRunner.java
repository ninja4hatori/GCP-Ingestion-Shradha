import java.io.IOException;
import java.lang.String;

public class JobRunner {
    public static void main(String[] args) {
        System.out.println("Initializing Variables...");
        String jsonPath = "C:\\Users\\Rahul\\Desktop\\GCP\\Key\\My First Project-5bea8475ed1c.json";
        String datasetName = "SampleAvro";
        String tableName="Sample";
        String location = "asia-south1";
        String fPath = "C:\\Users\\Rahul\\IdeaProjects\\GCP-Ingestion\\src\\main\\resources\\SampleFiles\\userdata1.avro";
        String sPath = "C:\\Users\\Rahul\\IdeaProjects\\GCP-Ingestion\\src\\main\\resources\\OutputFiles\\";
        System.out.println("Parameters set:");
        System.out.println("Table: " + datasetName + "." + tableName);
        System.out.println("File: " + fPath);

        try {
            System.out.println("Setting GCP credentials...");
            LoadData.connect(jsonPath);

            System.out.println("Loading data into table...");
            long outputRows = LoadData.load(datasetName, tableName, location, fPath);
            System.out.println("Number of records successfully loaded: " + outputRows);

            System.out.println("Querying the data now...");
            LoadData.query(datasetName, tableName);

            System.out.println("Querying the data and saving it to local file at " + sPath);
            LoadData.saveToLocal(datasetName, tableName, sPath, jsonPath);

        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }

    }
}
