import { Module } from '@nestjs/common';
import { ConditionalModule } from '@nestjs/config';
import { DatabaseModule } from '../database/database.module';
import { KafkaModule } from '../mq/kafka/kafka.module';
import { RabbitMqModule } from '../mq/rabbitmq/rabbitmq.module';
import { RedisStreamModule } from '../mq/redis-stream/redis-stream.module';
import { OrderModule } from './order/order.module';
import { OrderProcessorService } from './order/order-processor.service';
import { BenchController } from './bench.controller';

// 환경 변수가 'false'일 때만 끈다(값이 없으면 켬)
const enabled = (key: string) => (env: NodeJS.ProcessEnv) => env[key]?.toLowerCase() !== 'false';
const mqOptions = { imports: [OrderModule], handler: OrderProcessorService };

@Module({
  imports: [
    DatabaseModule,
    OrderModule,
    ConditionalModule.registerWhen(KafkaModule.register(mqOptions), enabled('MQ_KAFKA_ENABLED')),
    ConditionalModule.registerWhen(RabbitMqModule.register(mqOptions), enabled('MQ_RABBITMQ_ENABLED')),
    ConditionalModule.registerWhen(RedisStreamModule.register(mqOptions), enabled('MQ_REDIS_ENABLED')),
  ],
  controllers: [BenchController],
})
export class BenchModule {}
