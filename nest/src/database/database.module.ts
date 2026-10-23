import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import databaseConfig from '../config/database.config';
import { OrderResultRepository } from './order-result.repository';

@Module({
  imports: [ConfigModule.forFeature(databaseConfig)],
  providers: [OrderResultRepository],
  exports: [OrderResultRepository],
})
export class DatabaseModule {}
