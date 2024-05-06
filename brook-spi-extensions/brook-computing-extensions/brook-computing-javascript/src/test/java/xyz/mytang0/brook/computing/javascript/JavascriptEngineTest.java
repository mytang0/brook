package xyz.mytang0.brook.computing.javascript;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JavascriptEngineTest {

    private JavascriptEngine engine;

    @Before
    public void setUp() {
        this.engine = new JavascriptEngine();
    }

    @Test
    public void testCompute() throws Exception {
        Map<String, Object> input = new HashMap<>();
        input.put("env", "dev");
        Assert.assertTrue((Boolean) engine.compute("$.env == 'dev'", input));
    }
}