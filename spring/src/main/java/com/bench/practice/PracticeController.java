package com.bench.practice;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * MQ 연습용 API. 벤치마크와 별개인 practice-topic, practice:stream, practice-queue만 다룬다.
 * 사용법: practice/README.md
 */
@RestController
@RequestMapping("/practice")
public class PracticeController {
    private final Map<String, PracticeOps> ops;

    public PracticeController(List<PracticeOps> ops) {
        this.ops = ops.stream().collect(Collectors.toMap(PracticeOps::type, Function.identity()));
    }

    private PracticeOps of(String mq) {
        PracticeOps o = ops.get(mq);
        if (o == null) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "mq must be one of " + ops.keySet());
        return o;
    }

    public record PublishRequest(String key, String payload) {}

    // POST /practice/kafka/messages  {"key":"user-1","payload":"hello"}
    @PostMapping("/{mq}/messages")
    public Map<String, Object> publish(@PathVariable String mq, @RequestBody PublishRequest body) {
        PracticeOps o = of(mq);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("mq", mq);
        m.put("destination", o.destination());
        m.putAll(o.publish(body.key(), body.payload() == null ? "" : body.payload()));
        return m;
    }

    // GET /practice/kafka/messages?count=10&ack=true
    @GetMapping("/{mq}/messages")
    public Map<String, Object> read(@PathVariable String mq, @RequestParam(defaultValue = "10") int count,
                                    @RequestParam(defaultValue = "true") boolean ack) {
        PracticeOps o = of(mq);
        return Map.of("mq", mq, "destination", o.destination(), "messages", o.read(Math.max(1, Math.min(count, 100)), ack));
    }

    @GetMapping("/{mq}/info")
    public Map<String, Object> info(@PathVariable String mq) {
        PracticeOps o = of(mq);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("mq", mq);
        m.put("destination", o.destination());
        m.putAll(o.info());
        return m;
    }

    @DeleteMapping("/{mq}")
    public Map<String, Object> reset(@PathVariable String mq) {
        PracticeOps o = of(mq);
        o.reset();
        return Map.of("mq", mq, "deleted", o.destination());
    }
}
