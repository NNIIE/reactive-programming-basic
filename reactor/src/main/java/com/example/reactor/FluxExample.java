package com.example.reactor;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.stream.Stream;

public class FluxExample {

    static void main() {
//        basic();
        fromStream();
        generate();
        create();
        map();
        flatMap();
        range();
        fluxAndMono();
    }

    static void basic() {
        // Flux: 여러개의 데이터
        Flux<Integer> flux = Flux.just(1, 2, 3, 4, 5);
        flux.subscribe(data -> System.out.println("Received: " + data));

        System.out.println("------------");

        // Flux 연속된 데이터
        Flux<Integer> range = Flux.range(1, 5);
        range.subscribe(System.out::println);

        System.out.println("------");

        List<String> list = List.of("a", "b", "c");
        Flux<String> fromList = Flux.fromIterable(list);
        fromList.subscribe(System.out::println);
    }

    static void fromStream() {
        Flux<Integer> fromStream = Flux.fromStream(Stream.of(10, 20, 30));
        fromStream.subscribe(System.out::println);
    }

    // generate: 상태를 유지하면서 하나씩 생성, 한번에 하나씩만 생성하는 동기적인 방식
    static void generate() {
        Flux<Integer> generated = Flux.generate(
                () -> 0, // 초기 상태
                (state, sink) -> {
                    sink.next(state); // 데이터 발행
                    if (state == 4) {
                        sink.complete(); // 완료 신호
                    }
                    return state + 1; // 다음 상태
                });

        generated.subscribe(data -> System.out.println("generate: " + data));
    }

    static void create() {
        Flux<String> created = Flux.create(sink -> {
            sink.next("첫번쨰");
            sink.next("두번쨰");
            sink.next("세번쨰");
            sink.complete();
        });

        created.subscribe(System.out::println);
    }

    static void map() {
        Flux<String> upperCase = Flux.just("apple", "banana", "cherry")
                .map(String::toUpperCase);

        upperCase.subscribe(System.out::println);

        Flux<Integer> lengths = Flux.just("hello", "world", "reactor")
                .map(String::toUpperCase)
                .map(String::length);

        lengths.subscribe(System.out::println);
    }

    static void flatMap() {
        Flux<String> result = Flux.just("user-1", "user-2", "user-3")
                .flatMap(FluxExample::findUserName);

        result.subscribe(System.out::println);

        Flux<String> expanded = Flux.just("hello", "world")
                .flatMap(word -> Flux.fromArray(word.split("")));

        expanded.subscribe(System.out::println);
    }

    static void range() {
        Flux<Integer> even = Flux.range(1, 10).filter(i -> i % 2 == 0);
        even.subscribe(System.out::println);

        Flux<String> result = Flux.just("apple", "avocado", "banana", "apricot", "cherry")
                .filter(s -> s.startsWith("a"))
                .map(String::toUpperCase);

        result.subscribe(System.out::println);
    }

    static void fluxAndMono() {
        // Flux → Mono: collectList로 모든 데이터를 List로 수집
        Mono<List<Integer>> listMono = Flux.just(1, 2, 3, 4, 5)
                .collectList();
        listMono.subscribe(list -> System.out.println("collectList: " + list));

        System.out.println("---");

        // Flux → Mono: next로 첫 번째 데이터만 가져옴
        Mono<String> firstMono = Flux.just("첫 번째", "두 번째", "세 번째")
                .next();
        firstMono.subscribe(data -> System.out.println("next: " + data));

        System.out.println("---");

        // Mono → Flux: flux()로 변환
        Flux<String> fromMono = Mono.just("단일 값")
                .flux();
        fromMono.subscribe(data -> System.out.println("flux: " + data));

        System.out.println("---");

        // Flux → Mono: count로 데이터 개수
        Mono<Long> countMono = Flux.range(1, 100).count();
        countMono.subscribe(count -> System.out.println("count: " + count));
    }


    static Mono<String> findUserName(String userId) {
        return  Mono.just("User-" + userId);
    }

}
