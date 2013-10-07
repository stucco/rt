
//import org.scalatest.FunSuite;

import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.rules.MethodRule;
import org.junit.rules.TestWatchman;
import org.junit.runners.model.FrameworkMethod;

import gov.ornl.stucco.rt.util.Loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.lang.Class;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Vertex;

@RunWith(JUnit4.class)
public class LoaderSuite{

  @Rule public TestName name = new TestName();
 
  //TODO: log output is not consistant with scala tests.  I can't find any reasonable way to fix this, but it seems like there should be one.
  private static Logger logger = LoggerFactory.getLogger(LoaderSuite.class);

  private static String templateDBPath = "./src/test/resources/graph.db.empty";
  private static String testDBPath = "./src/test/resources/graph.db.test";

  @BeforeClass 
  public static void onlyOnce() {
    System.out.println( "Running LoaderSuite tests" );
  	logger.info( "LoaderSuite:" );  //TODO should really put in test initialization or something.  Related to above.
  }

  @Before
  public void setup() throws IOException{
  	File src = new File(templateDBPath);
  	File dst = new File(testDBPath);
  	FileUtils.copyDirectory(src, dst);
  }

  @After
  public void teardown() throws IOException{
    File tgt = new File(testDBPath);
    FileUtils.deleteDirectory(tgt);
  }

  @Test
  public void basicVertexTest() {
    logger.info("{}: put a vertex into DB, then find it by its index", name.getMethodName());
    Loader l = new Loader(testDBPath);
    
    //TODO in the future we should think about using our API here, instead of using reflection/private methods.
    // also should consider if/how the loader & the API are related in general.
    Method getVertexMethod = null;
    
    
    try{
      getVertexMethod = Loader.class.getDeclaredMethod("getVertex", Object.class);
      getVertexMethod.setAccessible(true);
    }catch(NoSuchMethodException e){
      org.junit.Assert.fail("NoSuchMethodException thrown!");
    }
	


	String testGraph = "{\"vertices\":[{\"_id\":\"testnode\"}], \"edges\":[]}";

    l.load(testGraph);
    l.load(testGraph);

    Vertex v = null;
	try{
      v = (Vertex)getVertexMethod.invoke(l, "testnode");
      //v = l.getVertex("testnode");
    }catch( IllegalAccessException e){
      org.junit.Assert.fail(" IllegalAccessException thrown!");
    }catch( InvocationTargetException e){
      Exception f = (Exception)e.getCause();
      f.printStackTrace();
      org.junit.Assert.fail(" InvocationTargetException thrown!  Cause was: " + f);
    }


  	org.junit.Assert.assertFalse("failure - vertex should not be null", v == null);

  	org.junit.Assert.assertFalse("failure - test fail msg", true);
    //Loader l = new Loader(location);
  }

}