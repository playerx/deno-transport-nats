import { NatsTransport } from '../nats.transport.ts'
import { assertEquals } from '../../testDeps.ts'

Deno.test('should connect to nats', async () => {
  let i = 0

  const transport = new NatsTransport(
    {
      moduleName: 'Test',
    },
    {
      newId: () => Date.now().toString() + (i++).toString(),
      jsonEncode: JSON.stringify,
      jsonDecode: JSON.parse,
    },
  )

  await transport.init()

  transport.on('PING', async () => {
    // console.log('ping received')

    return 'PONG'
  })

  await transport.start()

  const result = await transport.execute({
    route: 'PING',
    payload: {},
  })

  assertEquals(result, 'PONG')

  await transport.dispose()
})
