package com.example.reactor;

import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

public class SubscribeExample {

    static void main() {
        basic();
        baseSubscriber();
    }

    static void basic() {
        Flux<Integer> flux = Flux.just(1, 2, 3);

        // 1. 인자가 없음 : 데이터를 소비하지만, 아무것도 하지 않는
        flux.subscribe(); // -> 출력 x : 파이프라인은 실행

        // 2. onNext만 처리
        flux.subscribe(
                data -> System.out.println("Received: " + data));

        // 3. onNext + onError 처리
        flux.subscribe(
                data -> System.out.println("Received: " + data),
                error -> System.err.println("Error: " + error.getMessage()));

        // 4. onNext + onError + onComplete 처리
        flux.subscribe(
                data -> System.out.println("Received: " + data),
                error -> System.err.println("Error: " + error.getMessage()),
                () -> System.out.println("Completed"));
    }

    // 배압
    static void baseSubscriber() {
        // 일반적인 subscribe : request(Long.MAX_VALUE)로 모든 데이터를 요청

        Flux<Integer> flux = Flux.range(1, 10);

        flux.subscribe(new BaseSubscriber<>() {
            int count = 0;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                System.out.println("구독 시작 : 처음 3개 요청");
                request(3);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                System.out.println("받은 데이터: " + value);
                if (count % 3 == 0) {
                    System.out.println("3개 받음, 다음 3개 요청");
                    request(3);
                }
            }

            @Override
            protected void hookOnComplete() {
                System.out.println("모든 데이터 처리 완료");
            }

            @Override
            protected void hookOnError(Throwable throwable) {
                System.out.println("에러 발생: " + throwable.getMessage());
            }
        });
    }

}
