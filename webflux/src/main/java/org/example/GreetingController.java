package org.example;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
public class GreetingController {

    @GetMapping("/hello")
    public Mono<String> hello() {
        return Mono.just("Hello WebFlux!");
    }

    @GetMapping("/items1")
    public Flux<String> items1() {
        return Flux.just("item1", "item2", "item3");
    }

    @GetMapping("/items2")
    public Mono<List<String>> items2() {
        return Flux.just("item1", "item2", "item3").collectList();
    }

}
