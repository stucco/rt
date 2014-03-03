/**
 * 
 */
package gov.ornl.stucco.util;


import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.configuration.BaseConfiguration;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;

/**
 * @author euf
 *
 */
public class Loader {

	private TitanGraph graph;
	private Logger logger;
	
	public Loader(){
		logger = LoggerFactory.getLogger(Loader.class);

		BaseConfiguration conf = new BaseConfiguration();
		conf.setProperty("storage.backend","cassandra");
		conf.setProperty("storage.hostname","127.0.0.1");
		TitanGraph graph = TitanFactory.open(conf);

		Set<String> vKeys = graph.getIndexedKeys(Vertex.class);
		if( ! vKeys.contains("name") )
			graph.createKeyIndex("name",Vertex.class);

		Set<String> eKeys = graph.getIndexedKeys(Edge.class);
		if( ! eKeys.contains("name") )
			graph.createKeyIndex("name",Edge.class);
	}

	public void load(String subgraph){
		try {
			//System.out.println("HEY! loading graph: " + subgraph);
			logger.info("loading graph: " + subgraph);
			//g is the subgraph to add, in graphson format.
			JSONObject g = new JSONObject( subgraph );
			
			JSONArray verts = g.getJSONArray("vertices");
			JSONArray edges = g.optJSONArray("edges");
			
			for(int i=0; i<verts.length(); i++){
				JSONObject v = verts.getJSONObject(i);
				addVertex(v);
			}
			if(edges != null){
				for(int i=0; i<edges.length(); i++){
					JSONObject e = edges.getJSONObject(i);
					addEdge(e);
				}
			}
		}
		catch(IOException e){ //any transaction-related problems
			// TODO terrible catch block
			System.err.println("1: error! " + e);
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO terrible catch block
			System.err.println("2: error! " + e);
			e.printStackTrace();
		}
		finally
		{
			//tx.finish();
			graph.shutdown();
		}
	}
	
	//TODO: name vs. _id in vertex vs edge indices - I thought I cleaned that up?  guess not.
	private Vertex getVertex(Object vertexId) {
		Vertex vertex = null;
		Iterator<Vertex> vertexIterator = graph.getVertices("name",vertexId).iterator();
		if (vertexIterator.hasNext()) {
			vertex = vertexIterator.next();
		}
		return(vertex);
	}
	
	private Vertex addVertex(JSONObject v) throws IOException, JSONException{
		Object vertexId = v.optString("name");
		if(vertexId == ""){
			vertexId = v.getString("_id");
			v.putOnce("name", vertexId);
		}
		Vertex vertex = null;
		vertex = getVertex(vertexId);

		if (vertex == null) { //make new vertex if needed
			vertex = graph.addVertex(vertexId);
			logger.info("adding vertex: " + v.getString("name") + " " + vertex.toString() );
		
			//update vertex properties.
			String[] keys = JSONObject.getNames(v);
			for(int i=0; i<keys.length; i++){
				JSONArray arrVal = v.optJSONArray(keys[i]);
				if(arrVal != null){ //arrays need special handling: can't just assign a JSONArray as a property
					Set<String> vals = new HashSet<String>(); //TODO won't necessarily be strings ...
					for(int j=0; j<arrVal.length(); j++){
						vals.add(arrVal.getString(j));
					}
					vertex.setProperty(keys[i], vals);
				}
				else{
					Object val = v.get(keys[i]);
					vertex.setProperty(keys[i], val);
				}
			}
		
		}
		else{
			logger.info("updating vertex: " + v.getString("name") + " " + vertex.toString() );
			Set<String> originalKeys = vertex.getPropertyKeys();
			JSONObject originalObject = new JSONObject();
			try {
				originalObject = new JSONObject(GraphSONUtility.jsonFromElement(vertex, originalKeys, GraphSONMode.NORMAL).toString());
			} catch (org.codehaus.jettison.json.JSONException e) {
				e.printStackTrace();
			}
			logger.debug("original vertex as JSONObject: " + originalObject);
			
			//update vertex properties.
			String[] keys = JSONObject.getNames(v);
			for(int i=0; i<keys.length; i++){
				if ((!keys[i].equals("_id")) && (!keys[i].equals("name"))) {
					if (originalKeys.contains(keys[i])) {
						logger.debug("merging property for key '" + keys[i] + "'");
						
						Set<String> vals = new HashSet<String>(); //TODO won't necessarily be strings ...
						JSONArray originalArrVals = originalObject.optJSONArray(keys[i]);
						if (originalArrVals != null) {
							for (int j=0; j<originalArrVals.length(); j++) {
								vals.add(originalArrVals.getString(j));
							}
						}
						else {
							String val = String.valueOf(vertex.getProperty(keys[i]));
							vals.add(val);
						}
						
						JSONArray arrVal = v.optJSONArray(keys[i]);
						if(arrVal != null){ //arrays need special handling: can't just assign a JSONArray as a property
							for(int j=0; j<arrVal.length(); j++){
								vals.add(arrVal.getString(j));
							}
						}
						else{
							String val = String.valueOf(v.get(keys[i]));
							vals.add(val);
						}
						 //don't put a single value into an array
						if ((!vals.isEmpty()) && (vals.size() > 1)) {
							logger.debug("merged array property value: " + vals);
							vertex.setProperty(keys[i], vals);
						}
						else if (vals.size() == 1){
							String singleVal = vals.iterator().next();
							logger.debug("merged single property value: " + singleVal);
							vertex.setProperty(keys[i], singleVal);
						}
						
						originalKeys.remove(keys[i]);
						
					}
					else {
						logger.debug("creating property for key '" + keys[i] + "'");
						JSONArray arrVal = v.optJSONArray(keys[i]);
						if(arrVal != null){ //arrays need special handling: can't just assign a JSONArray as a property
							Set<String> vals = new HashSet<String>(); //TODO won't necessarily be strings ...
							for(int j=0; j<arrVal.length(); j++){
								vals.add(arrVal.getString(j));
							}
							vertex.setProperty(keys[i], vals);
						}
						else{
							Object val = v.get(keys[i]);
							vertex.setProperty(keys[i], val);
						}
					}
				}
				
			}
			//any property of the original vertex that was not in the new set of properties, add back
			for (String propertyKey : originalKeys) {
				String originalValue = String.valueOf(vertex.getProperty(propertyKey));
				vertex.setProperty(propertyKey, originalValue);
				logger.debug("setting key '" + propertyKey + "' back to value '" + originalValue + "'");
			}
		}
		logger.debug("adding vertex: " + String.valueOf(vertex.getProperty("name")) + " " + vertex.toString());	
		return(vertex);
	}
	
	private Edge getEdge(Object edgeId) {
		Edge edge = null;
		Iterator<Edge> edgeIterator = graph.getEdges("_id", edgeId).iterator();
		if (edgeIterator.hasNext()) {
			edge = edgeIterator.next();
		}
		return(edge);
	}
	
	private Edge addEdge(JSONObject e) throws IOException, JSONException {
		Object edgeId = e.getString("_id");
		Object dstVertexId = e.getString("_inV");
		Object srcVertexId = e.getString("_outV");
		Edge edge = getEdge(edgeId);
		if (edge == null) { //TODO this will avoid dupe edges, but will not update edges with new properties.  ok for now, but should change.
			
			Vertex srcVertex = getVertex(srcVertexId);
			if (srcVertex == null) {
				String note = "Source vertex not found '" + srcVertexId + "'. Creating placeholder vertex.";
				//logger.error(error);
				logger.info(note);
				
				JSONObject v = new JSONObject();
				v.put("_id", e.getString("_outV")); //TODO this name vs _id issue is kind of dumb ... Is it really still needed?
				v.put("name", e.getString("_outV"));
				addVertex(v);
				
				srcVertex = getVertex(srcVertexId); //TODO feels hacky
			}

			Vertex dstVertex = getVertex(dstVertexId);
			if (dstVertex == null) {
				String note = "Source vertex not found '" + dstVertexId + "'. Creating placeholder vertex.";
				//logger.error(error);
				logger.info(note);
				
				JSONObject v = new JSONObject();
				v.put("_id", e.getString("_inV"));
				v.put("name", e.getString("_inV"));
				addVertex(v);
				
				dstVertex = getVertex(dstVertexId); //TODO feels hacky
			}
			
			edge = graph.addEdge(null, srcVertex, dstVertex, e.getString("_label"));
			String[] keys = JSONObject.getNames(e);
			for(int i=0; i<keys.length; i++){
				JSONArray arrVal = e.optJSONArray(keys[i]);
				if(arrVal != null){ //arrays need special handling: can't just assign a JSONArray as a property
					Set<String> vals = new HashSet<String>(); //TODO won't necessarily be strings ...
					for(int j=0; j<arrVal.length(); j++){
						vals.add(arrVal.getString(j));
					}
					edge.setProperty(keys[i], vals);
				}
				else{
					Object val = e.get(keys[i]);
					edge.setProperty(keys[i], val);
				}
			}
		}
		else {
			logger.info("edge already exists... merging properties");
			Set<String> originalKeys = edge.getPropertyKeys();
			JSONObject originalObject = new JSONObject();
			try {
				originalObject = new JSONObject(GraphSONUtility.jsonFromElement(edge, originalKeys, GraphSONMode.NORMAL).toString());
			} catch (org.codehaus.jettison.json.JSONException ex) {
				ex.printStackTrace();
			}
			logger.debug("original edge as JSONObject: " + originalObject);
			
			//update vertex properties.
			String[] keys = JSONObject.getNames(e);
			for(int i=0; i<keys.length; i++){
				if ((!keys[i].equals("_id")) && (!keys[i].equals("_inV")) && (!keys[i].equals("_outV"))) {
					if (originalKeys.contains(keys[i])) {
						logger.debug("merging property for key '" + keys[i] + "'");
						
						Set<String> vals = new HashSet<String>(); //TODO won't necessarily be strings ...
						JSONArray originalArrVals = originalObject.optJSONArray(keys[i]);
						if (originalArrVals != null) {
							for (int j=0; j<originalArrVals.length(); j++) {
								vals.add(originalArrVals.getString(j));
							}
						}
						else {
							String val = String.valueOf(edge.getProperty(keys[i]));
							vals.add(val);
						}
						
						JSONArray arrVal = e.optJSONArray(keys[i]);
						if(arrVal != null){ //arrays need special handling: can't just assign a JSONArray as a property
							for(int j=0; j<arrVal.length(); j++){
								vals.add(arrVal.getString(j));
							}
						}
						else{
							String val = String.valueOf(e.get(keys[i]));
							vals.add(val);
						}
						 //don't put a single value into an array
						if ((!vals.isEmpty()) && (vals.size() > 1)) {
							logger.debug("merged array property value: " + vals);
							edge.setProperty(keys[i], vals);
						}
						else if (vals.size() == 1){
							String singleVal = vals.iterator().next();
							logger.debug("merged single property value: " + singleVal);
							edge.setProperty(keys[i], singleVal);
						}
						
						originalKeys.remove(keys[i]);
						
					}
					else {
						logger.debug("creating property for key '" + keys[i] + "'");
						JSONArray arrVal = e.optJSONArray(keys[i]);
						if(arrVal != null){ //arrays need special handling: can't just assign a JSONArray as a property
							Set<String> vals = new HashSet<String>(); //TODO won't necessarily be strings ...
							for(int j=0; j<arrVal.length(); j++){
								vals.add(arrVal.getString(j));
							}
							edge.setProperty(keys[i], vals);
						}
						else{
							Object val = e.get(keys[i]);
							edge.setProperty(keys[i], val);
						}
					}
				}
				
			}
			//any property of the original vertex that was not in the new set of properties, add back
			for (String propertyKey : originalKeys) {
				String originalValue = String.valueOf(edge.getProperty(propertyKey));
				edge.setProperty(propertyKey, originalValue);
				logger.debug("setting key '" + propertyKey + "' back to value '" + originalValue + "'");
			}
		}
		logger.debug("adding edge: " + edge.getProperty("_id") + " " + edge.toString() ); //TODO again w name v id...
		return(edge);
	}
	
}