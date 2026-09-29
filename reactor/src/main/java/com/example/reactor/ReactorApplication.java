package com.example.reactor;


import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

public class ReactorApplication {

	public static void main(String[] args) {
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

}
