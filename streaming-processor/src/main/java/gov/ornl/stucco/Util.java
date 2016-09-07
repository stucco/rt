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
		//logger.debug("Splitting graph of:\n" + graph.toString(2));
		Map<String, JSONObject> components = new HashMap<String, JSONObject>();
		//handle edges
		JSONArray edgesJsonArray = (JSONArray)graph.remove("edges");
		JSONObject edges = new JSONObject();
		for(int i=0; i<edgesJsonArray.length(); i++){
			JSONObject edge = (JSONObject) edgesJsonArray.get(i);
			String outV = "";
			if(edge.has("outVertID")){ //these first items are stucco-style
				outV = edge.getString("outVertID");
			}else if(edge.has("_outV")){ //these next items are graphson-style
				outV = edge.getString("_outV");
			}else{
				logger.warn("Could not find outV for edge: " + edge.toString());
			}
			String inV = "";
			if(edge.has("inVertID")){
				inV = edge.getString("inVertID");
			}else if(edge.has("_inV")){
				inV = edge.getString("_inV");
			}else{
				logger.warn("Could not find inV for edge: " + edge.toString());
			}
			String relation = "";
			if(edge.has("relation")){
				relation = edge.getString("relation");
			}else if(edge.has("_label")){
				relation = edge.getString("_label");
			}else{
				logger.warn("Could not find relation for edge: " + edge.toString());
			}
			String id = outV + "_" + relation + "_" + inV;
			edges.put(id, edge);
		}
		components.put("edges", edges);
		//handle vertices
		JSONObject vertices = new JSONObject();
		Object vertObj = graph.remove("vertices");
		try{
			//stucco-style graphs
			vertices = (JSONObject)vertObj;
			components.put("vertices", vertices);
		}catch(ClassCastException e){
			//graphson graphs
			JSONArray verticesJsonArray = (JSONArray)vertObj;
			for(int i=0; i<verticesJsonArray.length(); i++){
				JSONObject vert = (JSONObject) verticesJsonArray.get(i);
				String id = vert.getString("_id");
				vertices.put(id, vert);
			}
			components.put("vertices", vertices);
		}
		//TODO: split into smaller groups
		//Note that building maps in this way for verts/edges will prevent any duplicates, which the bulk-loader would not like.
		if(graph.keySet().size() > 0){
			logger.warn("Graphson item contained unexpected content.  Remaining content is:\n" + graph.toString(2));
		}
		return components;
	}
}
