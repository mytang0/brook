package xyz.mytang0.brook.computing.javascript;


import lombok.extern.slf4j.Slf4j;
import xyz.mytang0.brook.spi.computing.Engine;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

@Slf4j
public class JavascriptEngine implements Engine {

    static final String TYPE = "javascript";

    private final ScriptEngine engine;

    public JavascriptEngine() {
        ScriptEngineManager scriptEngineManager =
                new ScriptEngineManager(
                        JavascriptEngine.class.getClassLoader());
        engine = scriptEngineManager.getEngineByName(TYPE);
    }


    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Object compute(String source, Object input) throws Exception {
        Bindings bindings = engine.createBindings();
        bindings.put("$", input);
        return engine.eval(source, bindings);
    }
}
