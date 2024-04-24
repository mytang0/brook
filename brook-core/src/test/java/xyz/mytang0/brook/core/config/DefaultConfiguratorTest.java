package xyz.mytang0.brook.core.config;

import lombok.Data;
import org.apache.commons.lang3.Validate;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import xyz.mytang0.brook.common.extension.ExtensionDirector;
import xyz.mytang0.brook.common.extension.ExtensionLoader;
import xyz.mytang0.brook.common.utils.JsonUtils;
import xyz.mytang0.brook.spi.config.ConfigProperties;
import xyz.mytang0.brook.spi.config.ConfigSource;
import xyz.mytang0.brook.spi.config.ConfigValidator;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

public class DefaultConfiguratorTest {

    private final Map<String, Object> properties = new HashMap<>();

    private DefaultConfigurator configurator;


    @Before
    public void setup() {
        ExtensionLoader<ConfigSource> loader =
                ExtensionDirector
                        .getExtensionLoader(ConfigSource.class);

        loader.addExtension("test",
                TestConfigSource.class,
                new TestConfigSource(properties));

        configurator = new DefaultConfigurator();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRefreshConfig1() {
        configurator.refreshConfig(Interface.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRefreshConfig2() {
        configurator.refreshConfig(Abstract.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRefreshConfig3() {
        configurator.refreshConfig(Config1.class);
    }

    @Test(expected = RuntimeException.class)
    public void testRefreshConfig4() {
        properties.put("config2.name", "Brook");
        configurator.refreshConfig(Config2.class);
    }

    @Test(expected = RuntimeException.class)
    public void testRefreshConfig5() {
        properties.put("config3.name", "Brook");
        configurator.refreshConfig(Config3.class);
    }

    @Test
    public void testRefreshConfig6() {
        properties.put("config4.name", "Brook");
        properties.put("config4.desc", "Brook is an orchestration engine, supports microservices and in-app logic (embedded use) orchestration.");
        properties.put("config4.year", new GregorianCalendar().get(Calendar.YEAR) - 1900);
        properties.put("config4.embeddable", "true");
        properties.put("config4.duration", "PT10S");
        Config4 config4 = configurator.refreshConfig(Config4.class);
        Assert.assertNotNull(config4);
        Assert.assertEquals(properties.get("config4.name"), config4.getName());
        Assert.assertEquals(properties.get("config4.desc"), config4.getDesc());
        Assert.assertEquals(properties.get("config4.year"), config4.getYear());
        Assert.assertEquals(Boolean.parseBoolean(
                properties.get("config4.embeddable").toString()
        ), config4.isEmbeddable());
        Assert.assertEquals('B', config4.getCharacter().charValue());
        Assert.assertEquals(properties.get("config4.duration"), config4.getDuration().toString());
    }

    @Test
    public void testRefreshConfig7() {
        properties.put("config5.name", "Brook");
        Config5 config5 = configurator.refreshConfig(Config5.class);
        Assert.assertNotNull(config5);
        Assert.assertEquals(properties.get("config5.name"), config5.getName());

        Config6 config6 = config5.getConfig6();
        Assert.assertNull(config6);

        String inner = "object";
        Config6 origin = new Config6();
        origin.setInner(inner);
        properties.put("config5.config6", JsonUtils.toJsonString(origin));

        config5 = configurator.refreshConfig(Config5.class);
        config6 = config5.getConfig6();
        Assert.assertNotNull(config6);
        Assert.assertEquals(inner, config6.getInner());

        // Contains escape characters.
        properties.put("config5.config6", "{\"inner\":\"" + inner + "\"}");
        config5 = configurator.refreshConfig(Config5.class);
        config6 = config5.getConfig6();
        Assert.assertNotNull(config6);
        Assert.assertEquals(inner, config6.getInner());
    }

    @Test
    public void testRefreshConfig8() {
        properties.put("config7.name", "Brook");
        Config7 config7 = configurator.refreshConfig(Config7.class);
        Assert.assertNotNull(config7);
        Assert.assertEquals(properties.get("config7.name"), config7.getName());

        Config6 config6 = config7.getConfig6();
        Assert.assertNull(config6);

        String inner = "object";
        Config6 origin = new Config6();
        origin.setInner(inner);
        properties.put("config7.config6.inner", origin.getInner());

        config7 = configurator.refreshConfig(Config7.class);
        config6 = config7.getConfig6();
        Assert.assertNotNull(config6);
        Assert.assertEquals(inner, config6.getInner());
    }

    @Test
    public void testRefreshConfig9() {
        properties.put("config8.name", "Brook");
        Config8 config8 = configurator.refreshConfig(Config8.class);
        Assert.assertNotNull(config8);
        Assert.assertEquals(properties.get("config8.name"), config8.getName());

        Config6 config6 = config8.getConfig6();
        Assert.assertNotNull(config6);

        String inner = "object";
        Config6 origin = new Config6();
        origin.setInner(inner);
        properties.put("config8.config.inner", origin.getInner());

        config8 = configurator.refreshConfig(Config8.class);
        config6 = config8.getConfig6();
        Assert.assertNotNull(config6);
        Assert.assertEquals(inner, config6.getInner());
    }

    @Test
    public void testGetConfig1() {
        properties.put("config8.name", "Brook");
        Config8 config8 = configurator.refreshConfig(Config8.class);
        Assert.assertNotNull(config8);
        Assert.assertEquals(properties.get("config8.name"), config8.getName());

        Config6 config6 = config8.getConfig6();
        Assert.assertNotNull(config6);

        String inner = "object";
        Config6 origin = new Config6();
        origin.setInner(inner);
        properties.put("config8.config.inner", origin.getInner());

        Config8 newConfig8 = configurator.getConfig(Config8.class);
        config6 = newConfig8.getConfig6();
        Assert.assertNotNull(config6);

        Assert.assertEquals(config8, newConfig8);
    }

    interface Interface {

    }

    abstract static class Abstract {

    }

    static class Config1 {

    }

    @Data
    @ConfigProperties(prefix = "config2")
    static class Config2 {

        private String name;

        private Config2() {

        }
    }

    @Data
    @ConfigProperties(prefix = "config3")
    static class Config3 {

        private String name;

        public Config3(String s) {

        }
    }

    @Data
    @ConfigProperties(prefix = "config4", validator = Config4.Config4ConfigValidator.class)
    static class Config4 {

        private String name;

        private String desc;

        private Integer year;

        private boolean embeddable;

        private Character character = 'B';

        private Duration duration;

        static class Config4ConfigValidator implements ConfigValidator<Config4> {

            @Override
            public void validate(@Nullable Config4 target) {
                Validate.notNull(target);
                Validate.notBlank(target.getName());
            }
        }
    }

    @Data
    @ConfigProperties(prefix = "config5")
    static class Config5 {

        private String name;

        private Config6 config6;
    }

    @Data
    static class Config6 {

        private String inner;
    }

    @Data
    @ConfigProperties(prefix = "config7")
    static class Config7 {

        private String name;

        private Config6 config6;
    }

    @Data
    @ConfigProperties(prefix = "config8")
    static class Config8 {

        private String name;

        @ConfigProperties(prefix = "config")
        private Config6 config6;
    }

    static class TestConfigSource implements ConfigSource {

        private final Map<String, Object> properties;

        public TestConfigSource(Map<String, Object> properties) {
            this.properties = properties;
        }

        @Nullable
        @Override
        public Object getProperty(String key) {
            return properties.get(key);
        }
    }
}