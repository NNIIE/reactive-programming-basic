package org.example;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;

@Component
public class TimeWebSocketHandler implements WebSocketHandler {


    @Override
    public Mono<Void> handle(WebSocketSession session) {
        // 클라이언트로 1초마다 시간 전송
        Flux<WebSocketMessage> serverToClient = Flux.interval(Duration.ofSeconds(1))
                .map(tick -> session.textMessage("서버 시간: " + LocalDateTime.now().toLocalTime()));

        // 클라이언트에서 오는 메시지도 수신 (소비만 하고 종료신호를 전달)
        Mono<Void> clientToServer = session.receive()
                .doOnNext(msg -> System.out.println("클라이언트: " + msg.getPayloadAsText()))
                .then();

        // 두 스트림을 함께 실행
        return Mono.zip(session.send(serverToClient), clientToServer).then();
    }

}
