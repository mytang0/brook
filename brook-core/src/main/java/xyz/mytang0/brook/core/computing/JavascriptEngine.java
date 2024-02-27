package xyz.mytang0.brook.core.computing;


import xyz.mytang0.brook.spi.computing.Engine;
import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import lombok.extern.slf4j.Slf4j;

import javax.script.Bindings;
import javax.script.ScriptEngine;

import static xyz.mytang0.brook.core.constants.FlowConstants.DEFAULT_ENGINE_TYPE;

@Slf4j
public class JavascriptEngine implements Engine {

    private static final ScriptEngine ENGINE = new NashornScriptEngineFactory().getScriptEngine(
            new String[]{"-doe", "--global-per-engine", "-timezone=Asia/Shanghai"},
            JavascriptEngine.class.getClassLoader());

    static final String TYPE = DEFAULT_ENGINE_TYPE;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Object compute(String source, Object input) throws Exception {
        Bindings bindings = ENGINE.createBindings();
        bindings.put("$", input);
        return ENGINE.eval(source, bindings);
    }
}
