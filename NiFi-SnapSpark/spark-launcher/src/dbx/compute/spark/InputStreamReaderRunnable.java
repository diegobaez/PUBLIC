package dbx.compute.spark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collection;

public class InputStreamReaderRunnable implements Runnable {

    private BufferedReader reader;
    private String name;
    private Collection returnData;

    public InputStreamReaderRunnable(InputStream is, String name, Collection retData) {
        this.reader = new BufferedReader(new InputStreamReader(is));
        this.name = name;
        this.returnData = retData;
    }

    
    public void run() {
        System.out.println("InputStream " + name + ":");
        try {
            String line = reader.readLine();
            this.returnData.add(line);
            while (line != null) {
                //System.out.println("DATA:"+line);
                line = reader.readLine();
                this.returnData.add(line);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
