import { ModuleMetadata, Type } from '@nestjs/common';

/** 발행할 메시지. key는 Kafka 파티션 결정에만 쓰고, RabbitMQ와 Redis Streams는 무시한다. */
export interface Message {
  key?: string;
  payload: string;
}

/** 컨슈머가 받은 메시지를 넘기는 대상. 애플리케이션이 구현한다. 예외를 던지면 ack하지 않는다. */
export interface MessageHandler {
  handle(payload: string): Promise<void>;
}

/** MQ별 발행 구현의 공통 인터페이스 */
export interface MessagePublisher {
  /** 'kafka' | 'rabbit' | 'redis' */
  readonly type: string;
  publish(messages: Message[]): Promise<void>;
  /** 큐/스트림에 남은 메시지를 비운다 */
  purge(): Promise<void>;
}

export const MESSAGE_HANDLER = Symbol('MESSAGE_HANDLER');

/** MQ 모듈의 register() 옵션. handler 클래스는 imports의 모듈이 export해야 한다. */
export interface MqModuleOptions {
  imports?: ModuleMetadata['imports'];
  handler: Type<MessageHandler>;
}
