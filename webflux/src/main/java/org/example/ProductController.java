package org.example;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
public class ProductController {

    @GetMapping("/products/first")
    public Mono<Product> getFirst() {
        return Mono.just(new Product("1", "노트북", 1500000));
    }

    @GetMapping("/products")
    public Flux<Product> getAll() {
        return Flux.just(
                new Product("1", "노트북", 1500000),
                new Product("2", "마우스", 50000),
                new Product("3", "키보드", 150000)
        );
    }

    @GetMapping("/products/availbale")
    public Mono<ResponseEntity<Product>> getAllAvailable() {
        boolean inStock = true;
        Mono<Product> found = inStock
                ? Mono.just(new Product("1", "노트북", 1500000))
                : Mono.empty();

        return found
                .map(ResponseEntity::ok)
                .switchIfEmpty(Mono.just(ResponseEntity.notFound().build()));
    }

}
