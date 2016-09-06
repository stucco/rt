package gov.ornl.stucco;

import gov.ornl.stucco.workers.StructuredWorker;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

	private static final Logger logger = LoggerFactory.getLogger(Util.class);
	
	public static Map<String, JSONObject> splitGraph(JSONObject graph) {
		Map<String, JSONObject> components = new HashMap<String, JSONObject>();
		JSONArray edgesJsonArray = (JSONArray)graph.remove("edges");
		JSONArray verticesJsonArray = (JSONArray)graph.remove("vertices");
		if(graph.keySet().size() > 0){
			logger.warn("Graphson item contained unexpected content.  Remaining content is:\n" + graph.toString(2));
		}
		//handle edges
		JSONObject edges = new JSONObject();
		for(int i=0; i<edgesJsonArray.length(); i++){
			JSONObject edge = (JSONObject) edgesJsonArray.get(i);
			String id = edge.getString("_outV") + "_" + edge.getString("_label") + "_" + edge.getString("_inV");
			edges.put(id, edge);
		}
		components.put("edges", edges);
		//handle vertices
		JSONObject vertices = new JSONObject();
		for(int i=0; i<verticesJsonArray.length(); i++){
			JSONObject vert = (JSONObject) verticesJsonArray.get(i);
			String id = vert.getString("_id");
			vertices.put(id, vert);
		}
		components.put("vertices", vertices);
		//TODO: split into smaller groups
		//Note that building maps in this way for verts/edges will prevent any duplicates, which the bulk-loader would not like.
		return components;
	}
}
