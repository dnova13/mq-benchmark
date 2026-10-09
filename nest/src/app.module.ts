import { Module } from '@nestjs/common';
import { BenchController } from './bench.controller';
import { KafkaService } from './kafka.service';
import { RedisStreamService } from './redis-stream.service';
import { RabbitService } from './rabbit.service';
import { DbService } from './db.service';
import { RunService } from './run.service';
import { ProcessorService } from './processor.service';

@Module({
  controllers: [BenchController],
  providers: [KafkaService, RedisStreamService, RabbitService, DbService, RunService, ProcessorService],
})
export class AppModule {}
