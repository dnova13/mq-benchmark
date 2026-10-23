package com.bench.mq.common;

/**
 * 컨슈머가 받은 메시지를 넘기는 대상. 애플리케이션이 하나를 빈으로 등록한다.
 * 예외를 던지면 ack하지 않는다(재전달 여부는 MQ별 설정을 따른다).
 */
@FunctionalInterface
public interface MessageHandler {
    void handle(String payload);
}
