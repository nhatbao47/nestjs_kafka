import { Controller, Get, Inject, OnModuleInit } from '@nestjs/common';
import { ClientKafka, EventPattern } from '@nestjs/microservices';
import { AppService } from './app.service';
 
import {
  SchemaRegistry,
  readAVSCAsync,
} from "@kafkajs/confluent-schema-registry";
 
// declaring a TypeScript type for our message structure
declare type MyMessage = {
  orderId: string;
  userId: string;
  price: number
};
const registry = new SchemaRegistry({
  host: "http://localhost:8081",
});
// This will create an AVRO schema from an .avsc file
const registerSchema = async () => {
  try {
    const schema = await readAVSCAsync("./avro/schema.avsc");    
    const { id } = await registry.register(schema);
    return id;
  } catch (e) {
    console.log(e);
  }
};
 
@Controller()
export class AppController implements OnModuleInit {
  constructor(
    private readonly appService: AppService,
    @Inject('AUTH_SERVICE') private readonly authClient: ClientKafka,
  ) {}
 
  @Get()
  getHello(): string {
    return this.appService.getHello();
  }
 
  @EventPattern('order_created')
  async handleOrderCreated(data: any ) {
    try {
      const registryId = await registerSchema();            
    if (registryId) {
      const message: MyMessage = await registry.decode(data);          
      this.appService.handleOrderCreated(message);
    }     
  
    } catch (error) {
      console.log(error);
    }
    
  }
 
  onModuleInit() {
    this.authClient.subscribeToResponseOf('get_user');
  }
}