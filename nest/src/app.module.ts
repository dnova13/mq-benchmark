import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { BenchModule } from './bench/bench.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }), // .env 로드. 각 모듈은 forFeature로 자기 설정만 등록한다
    BenchModule,
  ],
})
export class AppModule {}
