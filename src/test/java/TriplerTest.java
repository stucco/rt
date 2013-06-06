import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class TriplerTest {

  @Test
  public void tripleTest() {
    assertEquals("triple(5)", 15, Tripler.triple(5));
    assertEquals("triple(0)", 0, Tripler.triple(0));
  }
}
