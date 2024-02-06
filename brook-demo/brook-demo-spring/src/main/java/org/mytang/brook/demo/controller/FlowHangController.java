package org.mytang.brook.demo.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Just for testing.
 */
@RestController
@RequestMapping("/hang/test")
public class FlowHangController {

    private final Map<String, AtomicInteger> emulator = new ConcurrentHashMap<>();

    @PostMapping("/async")
    public String async() {
        String id = UUID.randomUUID().toString();
        emulator.put(id, new AtomicInteger(10));
        return id;
    }

    @GetMapping("/success")
    public String success(@RequestParam("id") String id) {
        AtomicInteger counter = emulator.get(id);
        if (counter != null) {
            if (0 < counter.decrementAndGet()) {
                return "fail";
            }
            emulator.remove(id);
        }
        return "ok";
    }

    @GetMapping("/fail")
    public String fail(@RequestParam("id") String id) {
        return "fail";
    }
}
