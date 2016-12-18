package training;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Utils {
    private static Properties props = null;

    static {
        props = new Properties();
        String path = Utils.class.getResource("/configuration.properties").getPath();
        try {
            props.load(new FileInputStream(path));
        } catch (IOException e) {
            System.err.println("Failed to load configuration.properties");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static final String DATA_DIRECTORY_PATH = props.getProperty("dataDirectoryPath") + "/";
    public static final String CONSUMER_KEY = props.getProperty("consumerKey");
    public static final String CONSUMER_SECRET = props.getProperty("consumerSecret");
    public static final String ACCESS_TOKEN = props.getProperty("accessToken");
    public static final String ACCESS_TOKEN_SECRET = props.getProperty("accessTokenSecret");
}
