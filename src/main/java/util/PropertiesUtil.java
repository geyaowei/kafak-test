package util;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {

    public static Properties readProperties(){
        try{
            Properties props=new Properties();
            String path = System.getProperty("user.dir");
            InputStream in=new FileInputStream(new File(System.getProperty("user.dir")+"/src/main/resources/application.properties"));
            props.load(in);
            return props;
        }catch(Exception e){
            System.out.println("properties load failure");
        }
        return null;
    }
}
