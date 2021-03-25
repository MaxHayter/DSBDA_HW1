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
 * A screen that includes areas and their names
 */
public class Screen {

    /**
     * Mapping area names to their locations
     */
    private final HashMap<String, Area> areas;

    /**
     * Constructor
     * @param configuration hadoop configuration class
     * @param path hadoop fs path
     * @throws IOException when file can not be opened or file has incorrect internal structure
     */
    public Screen(Configuration configuration, Path path) throws IOException {
        Pattern splitStr = Pattern.compile("[-;:]");
        areas = new HashMap<>();
        FileSystem fs = FileSystem.get(configuration);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FSDataInputStream(fs.open(path))));
        String line;
        Area area;
        while ((line = reader.readLine()) != null){
            try {
                String [] data = splitStr.split(line);
                area = new Area(new Pair(Integer.parseInt(data[0]), Integer.parseInt(data[1])),
                        new Pair(Integer.parseInt(data[2]), Integer.parseInt(data[3])));

                areas.put(data[4], area);
            }
            catch (NumberFormatException noexcept) {
                throw new IOException("Illegal input screen format");
            }
        }
    }

    /**
     * Method that uses the entered coordinates to determine the name of the area
     * @param coordinates screen coordinates
     * @return String name of the area
     */
    public String getAreaByCoordinates(Pair coordinates) {
        for (Map.Entry<String, Area> entry : areas.entrySet()) {
            Pair min = entry.getValue().getBegin();
            Pair max = entry.getValue().getEnd();
            if (coordinates.getX() >= min.getX() && coordinates.getY() >= min.getY() &&
                    coordinates.getX() <= max.getX() && coordinates.getY() <= max.getY()) {
                return entry.getKey();
            }
        }
        return "";
    }
}
