import { Module } from '@nestjs/common';
import { DatabaseModule } from '../../database/database.module';
import { OrderProcessorService } from './order-processor.service';
import { RunService } from './run.service';

@Module({
  imports: [DatabaseModule],
  providers: [RunService, OrderProcessorService],
  exports: [RunService, OrderProcessorService],
})
export class OrderModule {}
