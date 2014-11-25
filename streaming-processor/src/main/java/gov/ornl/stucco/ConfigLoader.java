package gov.ornl.stucco;

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
	
	public Map<String, Object> getConfig(String configHeading) {
		Yaml yamlReader = new Yaml();
		@SuppressWarnings("unchecked")
		Map<String, Map<String, Object>> configMap = (Map<String, Map<String, Object>>) yamlReader.load(ConfigLoader.class.getClassLoader().getResourceAsStream(configFile));

		Map<String, Object> subMap = configMap.get(configHeading);
		return subMap;
	}

}
