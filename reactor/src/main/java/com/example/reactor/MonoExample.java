package com.example.reactor;

import reactor.core.publisher.Mono;

public class MonoExample {

    static boolean featureEnabled = false;

    static void main() throws InterruptedException {
//        basic();
        just();
        supplier();
        callable();
        defer_1();
        defer_2();
    }

    static void basic() {
        // Mono: 하나의 데이터
        Mono<String> mono = Mono.just("Hello Reactor");
        mono.subscribe(data -> System.out.println("Received: " + data));

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
    }

     // 바로 실행, 이미 계산된 값을 Mono로 감싸야 할때 사용
    static void just() {
        Mono<String> justMono = Mono.just(getValue());
        System.out.println("Mono Created, but value not yet retrieved.");
        System.out.println("------------");
    }

    // Lazy Loding, 필요할때 사용하고 싶을때 사용
    static void supplier() {
        Mono<String> supplierMono = Mono.fromSupplier(MonoExample::getValue);
        System.out.println("Mono from supplier created, but value not yet retrieved.");
        supplierMono.subscribe(value -> System.out.println("Received value: " + value));
        System.out.println("------------");
    }

    // supplier과 동일하지만, checked exception을 던질 수 있음
    static void callable() {
        Mono<String> callableMono = Mono.fromCallable(() -> {
            if (Math.random() > 0.5) {
                throw new Exception("Random Exception Occurred.");
            }
            return "성공";
        });

        callableMono.subscribe(
                result -> System.out.println("결과: " + result),
                error -> System.err.println("에러: " + error.getMessage())
        );
        System.out.println("------------");
    }

    // Mono 자체의 생성을 지연 시킴
    static void defer_1() throws InterruptedException {
        Mono<Long> deferedMono = Mono.defer(() -> {
            System.out.println("생성중...");
            return Mono.just(System.currentTimeMillis());
        });

        System.out.println("Mono 생성 완료");
        deferedMono.subscribe(time -> System.out.println("첫 번째 구독: " + time));

        Thread.sleep(1000);

        System.out.println("1초 후에 두 번째 구독");
        deferedMono.subscribe(time -> System.out.println("두 번째 구독:" + time));
        System.out.println("------------");
    }

    // 구독 시점에 따라서 조건이 달라짐
    static void defer_2() {
        Mono<String> conditionMono = Mono.defer(() -> {
            if (featureEnabled) {
                return Mono.just("Feature is enabled");
            } else {
                return Mono.empty();
            }
        });

        System.out.println("Feature enabled: " + featureEnabled);

        conditionMono.subscribe(
                result -> System.out.println("Result: " + result),
                error -> System.err.println("Error: " + error),
                () -> System.out.println("Completed without emitting any value"));

        featureEnabled = true;

        conditionMono.subscribe(
                result -> System.out.println("Result: " + result),
                error -> System.err.println("Error: " + error),
                () -> System.out.println("Completed without emitting any value"));
    }

    static String getValue() {
        System.out.println("Getting value...");
        return "Hello, World!";
    }

}
