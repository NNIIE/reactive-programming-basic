package com.example.reactor;


import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public class ReactorApplication {

	public static void main(String[] args) {
		// Mono: 하나의 데이터
		Mono<String> mono = Mono.just("Hello Reactor");
		mono.subscribe(data -> System.out.println("Received: " + data));

		System.out.println("------------");

		// Flux: 여러개의 데이터
		Flux<Integer> flux = Flux.just(1, 2, 3, 4, 5);
		flux.subscribe(data -> System.out.println("Received: " + data));

		System.out.println("------------");

		// 빈 Mono
		Mono<String> emptyMono = Mono.empty();
		emptyMono.subscribe(
				data -> System.out.println("Received: " + data),
				error -> System.err.println("Error: " + error),
				() -> System.out.println("Completed")
		);

		System.out.println("------");

		// Mono Error
		Mono<String> errMono = Mono.error(new RuntimeException("Error!!"));
		errMono.subscribe(
				value -> System.out.println("Received: " + value),
				error -> System.err.println("Error: " + error.getMessage())
		);

		// Flux 연속된 데이터
		Flux<Integer> range = Flux.range(1, 5);
		range.subscribe(System.out::println);

		System.out.println("------");

        List<String> list = List.of("a", "b", "c");
		Flux<String> fromList = Flux.fromIterable(list);
		fromList.subscribe(System.out::println);
	}

}
