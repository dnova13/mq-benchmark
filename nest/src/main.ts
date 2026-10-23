import { NestFactory } from '@nestjs/core';
import { ConfigService } from '@nestjs/config';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.enableShutdownHooks(); // 종료 시 MQ, DB 연결을 닫는다
  await app.listen(app.get(ConfigService).get<number>('PORT', 3000));
}
bootstrap();
