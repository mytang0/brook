package xyz.mytang0.brook.spring.boot.computing;

import xyz.mytang0.brook.spi.computing.Engine;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelCompilerMode;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @link <a href="https://docs.spring.io/spring-framework/docs/3.2.x/spring-framework-reference/html/expressions.html">...</a>
 */
@Slf4j
public class SpelEngine implements Engine {

    private static final String TYPE = "spel";

    /**
     * The global compilerMode can be specified by configuring 'spring.expression.compiler.mode', details:
     *
     * @see SpelCompilerMode
     */
    private static final ExpressionParser parser = new SpelExpressionParser();

    private static final Cache<String, Expression> expressionCache = Caffeine
            .newBuilder()
            .maximumSize(500)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build();

    @Override
    public String type() {
        return TYPE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object compute(String source, Object input) {
        Expression engine = expressionCache.get(source, parser::parseExpression);
        assert engine != null;
        Object result;
        if (input instanceof Map) {
            StandardEvaluationContext evaluationContext = new StandardEvaluationContext();
            evaluationContext.setVariables((Map<String, Object>) input);
            result = engine.getValue(evaluationContext);
        } else {
            result = engine.getValue(input);
        }
        return result;
    }
}
