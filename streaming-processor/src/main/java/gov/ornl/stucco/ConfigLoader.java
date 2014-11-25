package gov.ornl.stucco;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class ConfigLoader {
	private String configFile;
	
	public ConfigLoader(){
		configFile = "config.yaml";
	}
	
	public ConfigLoader(String config){
		configFile = config;
	}
	
	@SuppressWarnings("unchecked")
	public Map<String, Object> getConfig(String configHeading) throws FileNotFoundException {
		Yaml yamlReader = new Yaml();
		Map<String, Map<String, Object>> configMap = null;
		Map<String, Object> subMap = null;
		configMap = (Map<String, Map<String, Object>>) yamlReader.load(new FileInputStream(configFile));
		subMap = configMap.get(configHeading);
		return subMap;
	}

}
