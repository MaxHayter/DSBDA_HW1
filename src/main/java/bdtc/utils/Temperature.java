package bdtc.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * A screen that includes the number of clicks and their names
 */
public class Temperature {
    /**
     * Mapping name of the temperature to its interval
     */
    private final HashMap<String, Pair> temperatures;

    /**
     * Constructor
     * @param configuration hadoop configuration class
     * @param path hadoop fs path
     * @throws IOException when file can not be opened or file has incorrect internal structure
     */
    public Temperature(Configuration configuration, Path path) throws IOException {
        Pattern splitStr = Pattern.compile("[-: ]+");
        temperatures = new HashMap<>();
        FileSystem fs = FileSystem.get(configuration);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FSDataInputStream(fs.open(path))));
        String line;
        while ((line = reader.readLine()) != null){
            try {
                String [] data = splitStr.split(line);
                temperatures.put(data[2], new Pair(Integer.parseInt(data[0]), Integer.parseInt(data[1])));
            }
            catch (NumberFormatException noexcept) {
                throw new IOException("Illegal input temperature format");
            }
        }
    }

    /**
     * Method that returns the name of the temperature based on the number of clicks entered
     * @param num number of clicks
     * @return String temperature name
     */
    public String getTemperatureByNum(int num) {
        for (Map.Entry<String, Pair> entry : temperatures.entrySet()) {
            if (num >= entry.getValue().getX() && num <= entry.getValue().getY()) {
                return entry.getKey();
            }
        }
        return "incredibly hot";
    }
}
