import { Injectable, OnApplicationShutdown } from "@nestjs/common";
import { Kafka, Consumer, ConsumerRunConfig } from "kafkajs";

@Injectable()
export class ConsumerService implements OnApplicationShutdown {
    private readonly kafka = new Kafka({
        brokers: [`${process.env.KAFKA_BROKER_HOST}:${process.env.KAFKA_BROKER_PORT}`],
    });
    private readonly consumers: Consumer[] = [];

    async consume(topics: string[], config: ConsumerRunConfig) {
        const consumer = this.kafka.consumer({ groupId: 'nestjs-kafka' });

        await consumer.connect();
        await consumer.subscribe({ topics });
        await consumer.run(config).catch(async (error) => {
            if (error.name === 'KafkaJSConnectionError') {
                setTimeout(async () => {
                    console.log('auto reconnecting...');
                    await consumer.connect();
                    await consumer.subscribe({ topics });
                    await consumer.run(config);
                }, 3000);
            }
        });
        this.consumers.push(consumer);
    }

    async onApplicationShutdown() {
        for (const consumer of this.consumers) {
            await consumer.disconnect();
        }
    }
}