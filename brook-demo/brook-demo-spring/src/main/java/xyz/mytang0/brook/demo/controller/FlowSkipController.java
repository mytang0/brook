package xyz.mytang0.brook.demo.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Just for testing.
 */
@RestController
@RequestMapping("/skip/test")
public class FlowSkipController {

    private final Map<String, AtomicInteger> emulator = new ConcurrentHashMap<>();

    @PostMapping("/work")
    public String work() {
        String id = UUID.randomUUID().toString();
        emulator.put(id, new AtomicInteger(100));
        return id;
    }

    @GetMapping("/query")
    public String query(String id) {
        AtomicInteger counter = emulator.get(id);
        if (counter != null && counter.decrementAndGet() > 0) {
            return "fail";
        }
        return "ok";
    }
}
