import { RabbitMQTransport } from '../rabbitmq.transport.ts'
import { assertEquals } from '../../testDeps.ts'

Deno.test('should connect to the rabbitmq', async () => {
  const transport = new RabbitMQTransport({
    amqpConnectionString: 'amqp://guest:guest@localhost:5672',
    publishExchangeName: 'deno-test-hub',
    moduleName: 'deno-test',
    isTestMode: true,
  })

  await transport.init()

  transport.on('PING', async () => {
    console.log('ping received')

    return 'PONG'
  })

  await transport.start()

  const result = await transport.execute({
    route: 'PING',
    payload: {},
  })

  assertEquals(result, 'PONG')
})
