import java.io.IOException;

public class JobRunner {
    public static void main(String[] args) {
        try {
            long outputRows = LoadData.load();
            System.out.println("Number of records successfully loaded: " + outputRows);
            System.out.println("Querying the data now.");
            LoadData.query();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
