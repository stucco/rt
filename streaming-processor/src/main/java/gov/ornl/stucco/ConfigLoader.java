package gov.ornl.stucco;

import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class ConfigLoader {
	private static final String CONFIG_FILE = "config.yaml";
	
	public static Map<String, Object> getConfig(String configHeading) {
		Yaml yamlReader = new Yaml();
		@SuppressWarnings("unchecked")
		Map<String, Map<String, Object>> configMap = (Map<String, Map<String, Object>>) yamlReader.load(ConfigLoader.class.getClassLoader().getResourceAsStream(CONFIG_FILE));

		Map<String, Object> subMap = configMap.get(configHeading);
		return subMap;
	}

}
