import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import kafkaConfig from '../config/kafka.config';
import rabbitmqConfig from '../config/rabbitmq.config';
import redisConfig from '../config/redis.config';
import { KafkaPracticeService } from './kafka-practice.service';
import { PracticeController } from './practice.controller';
import { RabbitMqPracticeService } from './rabbitmq-practice.service';
import { RedisPracticeService } from './redis-practice.service';

// 연습용 API. MQ 모듈과 접속 설정만 공유하고 연결은 따로 만든다(벤치마크 연결에 영향 없음)
@Module({
  imports: [ConfigModule.forFeature(kafkaConfig), ConfigModule.forFeature(redisConfig), ConfigModule.forFeature(rabbitmqConfig)],
  controllers: [PracticeController],
  providers: [KafkaPracticeService, RedisPracticeService, RabbitMqPracticeService],
})
export class PracticeModule {}
