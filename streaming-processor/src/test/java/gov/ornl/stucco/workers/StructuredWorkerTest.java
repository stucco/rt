package gov.ornl.stucco.workers;

import gov.ornl.stucco.Util;
import gov.ornl.stucco.workers.StructuredWorker;

import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import junit.framework.TestCase;

public class StructuredWorkerTest extends TestCase{
    public StructuredWorkerTest( String testName )
    {
        super( testName );
    }

    public void setUp(){
    }

    public void tearDown(){
    }
    
    public void testSplitGraph()
    {
		String testGraphsonString = "{\"vertices\":[" +
				"{" +
				"\"_id\":\"CVE-1999-0002\"," +
				"\"_type\":\"vertex\","+
				"\"source\":\"CVE\","+
				"\"vertexType\": \"vulnerability\"," +
				"\"description\":\"Buffer overflow in NFS mountd gives root access to remote attackers, mostly in Linux systems.\","+
				"\"references\":["+
				"\"CERT:CA-98.12.mountd\","+
				"\"http://www.ciac.org/ciac/bulletins/j-006.shtml\","+
				"\"http://www.securityfocus.com/bid/121\","+
				"\"XF:linux-mountd-bo\"],"+
				"\"status\":\"Entry\","+
				"\"score\":1.0"+
				"}," + 
				"{" +
				"\"availabilityImpact\": \"PARTIAL\"," +
				"\"accessVector\": \"NETWORK\"," +
				"\"cvssDate\": 1072933200," +
				"\"integrityImpact\": \"NONE\"," +
				//"\"vulnerableSoftware\": [\"cpe:/h:cabletron:smartswitch_router_8000_firmware:2.0\"]," +
				"\"accessComplexity\": \"LOW\"," +
				"\"modifiedDate\": 1220587200," +
				"\"vertexType\": \"vulnerability\"," +
				"\"_type\": \"vertex\"," +
				"\"references\":   [" +
				"\"http://razor.bindview.com/publish/advisories/adv_Cabletron.html\"," +
				"\"http://www.securityfocus.com/bid/841\"]," +
				"\"_id\": \"CVE-1999-1548\"," +
				"\"source\": \"NVD\"," +
				"\"description\": \"Cabletron SmartSwitch Router (SSR) 8000 firmware 2.x can only handle 200 ARP requests per second allowing a denial of service attack to succeed with a flood of ARP requests exceeding that limit.\"," +
				"\"cvssScore\": 5," +
				"\"publishedDate\": 943419600," +
				"\"confidentialityImpact\": \"NONE\"," +
				"\"accessAuthentication\": \"NONE\"" +
				"}," +	
				"{\"_id\":\"CVE-1999-nnnn\"," +
				"\"_type\":\"vertex\","+
				"\"source\":\"CVE\","+
				"\"vertexType\": \"vulnerability\"," +
				"\"description\":\"test description asdf.\","+
				"\"references\":[\"http://www.google.com\"],"+
				"\"status\":\"Entry\","+
				"\"score\":1.0"+
				"}],"+
				"\"edges\":[{"+ 
				"\"_id\":\"asdf\"," +
				"\"_inV\":\"CVE-1999-0002\"," +
				"\"_outV\":\"CVE-1999-nnnn\"," +
				"\"_label\":\"sameAs\","+
				"\"description\":\"some_value\""+
				"}," +
				"{" +
				"\"_id\":\"asdfAgain\"," +
				"\"_inV\":\"CVE-1999-0002\"," +
				"\"_outV\":\"CVE-1999-1548\"," +
				"\"_label\":\"sameAs\","+
				"\"description\":\"some_valueAgain\""+
				"}]}";
		//build and then split graph, check top-level structure
    	JSONObject graph = new JSONObject(testGraphsonString);
    	Map<String, JSONObject> components = Util.splitGraph(graph);
    	JSONObject edges = components.get("edges");
    	assertTrue(edges != null);
    	JSONObject vertices = components.get("vertices");
    	assertTrue(vertices != null);
    	//check edges
    	assertTrue(edges.getJSONObject("CVE-1999-nnnn_sameAs_CVE-1999-0002") != null);
    	assertTrue(edges.getJSONObject("CVE-1999-1548_sameAs_CVE-1999-0002") != null);
    	assertEquals(2, edges.length());
    	//check vertices
    	assertTrue(vertices.getJSONObject("CVE-1999-1548") != null);
    	assertTrue(vertices.getJSONObject("CVE-1999-0002") != null);
    	assertTrue(vertices.getJSONObject("CVE-1999-nnnn") != null);
    	assertEquals(3, vertices.length());
    }
}
