/** 연습용 API의 MQ별 구현. 벤치마크와 별개인 practice-* 대상만 다룬다. */
export interface PracticeOps {
  /** 'kafka' | 'redis' | 'rabbit' */
  readonly type: string;
  /** 실습 대상 이름(토픽, 스트림, 큐) */
  readonly destination: string;
  publish(key: string | undefined, payload: string): Promise<Record<string, unknown>>;
  /** 최근 메시지 조회. ack=false면 RabbitMQ도 메시지를 다시 큐에 넣는다(엿보기) */
  read(count: number, ack: boolean): Promise<Record<string, unknown>[]>;
  info(): Promise<Record<string, unknown>>;
  /** 실습 대상 삭제 */
  reset(): Promise<void>;
}
