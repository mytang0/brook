package org.mytang.brook.demo.service;

import org.mytang.brook.common.utils.JsonUtils;
import org.mytang.brook.common.utils.TimeUtils;
import org.mytang.brook.spring.boot.annotation.Taskable;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Component
public class DemoService {

    @Taskable(type = "test-void", description = "for testing")
    public void vd() {
    }

    @Taskable(type = "test-hello", description = "for testing")
    public String hello(String name) {
        return "Hello, " + name + " .";
    }

    @Taskable(type = "test-list", description = "for testing")
    public String list(List<Object> list) {
        return "list: " + JsonUtils.toJsonString(list);
    }

    @Taskable(type = "test-array", description = "for testing")
    public String array(Object[] array) {
        return "array: " + JsonUtils.toJsonString(array);
    }

    @Taskable(type = "test-map", description = "for testing")
    public String map(Map<String, Object> map) {
        return "map: " + JsonUtils.toJsonString(map);
    }

    @Taskable(type = "test-throw-npe", description = "for testing")
    public void throwNpe() {
        throw new NullPointerException();
    }

    @Taskable(type = "test-throw-iae", description = "for testing")
    public void throwIae() {
        throw new IllegalArgumentException();
    }

    @Taskable(type = "test-timeout", description = "for testing")
    public void timeout(Integer sleepSeconds) {
        if (sleepSeconds == null) {
            sleepSeconds = 5;
        }
        TimeUtils.sleep(sleepSeconds, TimeUnit.SECONDS);
    }
}
