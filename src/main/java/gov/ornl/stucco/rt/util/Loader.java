/**
 * 
 */
package gov.ornl.stucco.rt.util;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import com.tinkerpop.blueprints.impls.tg.TinkerGraphFactory;
import com.tinkerpop.blueprints.impls.neo4j.Neo4jGraph;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.util.io.graphson.GraphElementFactory;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONReader;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;

import org.json.*;
import org.neo4j.kernel.configuration.Config;

import org.slf4j.Logger;

/**
 * @author euf
 *
 */
public class Loader {
	private Neo4jGraph graph;
	private Index<Vertex> vertexIndex;
	private Index<Edge> edgeIndex;
	
	public void load(String subgraph, String dbLocation, Logger logger){        
		final Map<String, String> settings = new HashMap<String, String>();
		//it should default to "soft" anyway, but sometimes defaults to "gcr" instead depending on environment.  idk.
		settings.put("cache_type", "soft");
		graph = new Neo4jGraph(dbLocation, settings);
		
		vertexIndex = graph.getIndex("vName", Vertex.class);
		if (vertexIndex == null) {
			vertexIndex = graph.createIndex("vName", Vertex.class);
		}
		
		edgeIndex = graph.getIndex("eName", Edge.class);
		if (edgeIndex == null) {
			edgeIndex = graph.createIndex("eName", Edge.class);
		}
		
		try
		{
			//g is the subgraph to add, in graphson format.
			JSONObject g = new JSONObject( subgraph );
			
			JSONArray verts = g.getJSONArray("vertices");
			JSONArray edges = g.optJSONArray("edges");
			
			for(int i=0; i<verts.length(); i++){
				JSONObject v = verts.getJSONObject(i);
				addVertex(v, logger);
			}
			if(edges != null){
				for(int i=0; i<edges.length(); i++){
					JSONObject e = edges.getJSONObject(i);
					addEdge(e, logger);
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
	
	
	private Vertex getVertex(Object vertexId, Logger logger) {
		Vertex vertex = null;
		if (vertex == null) {
			Iterator<Vertex> vertexIterator = vertexIndex.get("name", vertexId).iterator();
			if (vertexIterator.hasNext()) {
				vertex = vertexIterator.next();
			}
		}
		return(vertex);
	}
	
	private Vertex addVertex(JSONObject v, Logger logger) throws IOException, JSONException{
		Object vertexId = v.getString("name");
		Vertex vertex = null;
		vertex = getVertex(vertexId, logger);

		if (vertex == null) { //make new vertex if needed
			vertex = graph.addVertex(vertexId);
			logger.info("adding vertex: " + v.getString("name") + " " + vertex.toString() );
		}else{
			logger.info("updating vertex: " + v.getString("name") + " " + vertex.toString() );
		}
		//update vertex properties.
		String[] keys = JSONObject.getNames(v);
		for(int i=0; i<keys.length; i++){
			JSONArray arrVal = v.optJSONArray(keys[i]);
			if(arrVal != null){ //arrays need special handling: can't just assign a JSONArray as a property
				String[] vals = new String[arrVal.length()]; //TODO won't necessarily be strings ...
				for(int j=0; j<vals.length; j++){
					vals[j] = arrVal.getString(j);
				}
				vertex.setProperty(keys[i], vals);
			}
			else{
				Object val = v.get(keys[i]);
				vertex.setProperty(keys[i], val);
			}
		}
		//TODO should remove old index if needed before adding this ...
		vertexIndex.put("name", v.getString("name"), vertex);
			
		return(vertex);
	}
	
	private Edge getEdge(Object edgeId, Logger logger) {
		Edge edge = null;
		Iterator<Edge> edgeIterator = edgeIndex.get("_id", edgeId).iterator();
		if (edgeIterator.hasNext()) {
			edge = edgeIterator.next();
		}
		return(edge);
	}
	
	private Edge addEdge(JSONObject e, Logger logger) throws IOException, JSONException {
		Object edgeId = e.getString("_id");
		Object dstVertexId = e.getString("_inV");
		Object srcVertexId = e.getString("_outV");
		Edge edge = getEdge(edgeId, logger);
		if (edge == null) { //TODO this will avoid dupe edges, but will not update edges with new properties.  ok for now, but should change.
			
			Vertex srcVertex = getVertex(srcVertexId, logger);
			if (srcVertex == null) {
				String note = "Source vertex not found '" + srcVertexId + "'. Creating placeholder vertex.";
				//logger.error(error);
				logger.info(note);
				
				JSONObject v = new JSONObject();
				v.put("_id", e.getString("_outV")); //TODO this name vs _id issue is kind of dumb ... Is it really still needed?
				v.put("name", e.getString("_outV"));
				addVertex(v, logger);
				
				srcVertex = getVertex(srcVertexId, logger); //TODO feels hacky
			}

			Vertex dstVertex = getVertex(dstVertexId, logger);
			if (dstVertex == null) {
				String note = "Source vertex not found '" + dstVertexId + "'. Creating placeholder vertex.";
				//logger.error(error);
				logger.info(note);
				
				JSONObject v = new JSONObject();
				v.put("_id", e.getString("_inV"));
				v.put("name", e.getString("_inV"));
				addVertex(v, logger);
				
				dstVertex = getVertex(dstVertexId, logger); //TODO feels hacky
			}
			
			edge = graph.addEdge(null, srcVertex, dstVertex, e.getString("_label"));
			String[] keys = JSONObject.getNames(e);
			for(int i=0; i<keys.length; i++){
				JSONArray arrVal = e.optJSONArray(keys[i]);
				if(arrVal != null){ //arrays need special handling: can't just assign a JSONArray as a property
					String[] vals = new String[arrVal.length()]; //TODO won't necessarily be strings ...
					for(int j=0; j<vals.length; j++){
						vals[j] = arrVal.getString(j);
					}
					edge.setProperty(keys[i], vals);
				}
				else{
					Object val = e.get(keys[i]);
					edge.setProperty(keys[i], val);
				}
			}
			logger.info("adding vertex: " + e.getString("_id") + edge.toString() ); //TODO again w name v id...
			edgeIndex.put("_id", e.getString("_id"), edge);
		}
		return(edge);
	}
	
}