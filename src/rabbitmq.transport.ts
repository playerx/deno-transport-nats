import {
  amqp,
  TransportBase,
  TransportOptions,
  TransportUtils,
  normalizeError,
  SendMessageProps,
  SendReplyMessageProps,
} from '../deps.ts'

export class RabbitMQTransport extends TransportBase {
  private channel: amqp.AmqpChannel = <any>null
  private queueName: string = ''
  private responseQueueName: string = ''
  private deadLetterQueueName: string = ''
  private uniqueIndex = 0

  constructor(
    protected options: TransportOptions & {
      moduleName: string
      amqpConnectionString: string
      publishExchangeName: string
      isTestMode?: boolean
    },
    protected utils: TransportUtils = {
      newId: () =>
        Date.now().toString() + (++this.uniqueIndex).toString(),
      jsonEncode: JSON.stringify,
      jsonDecode: JSON.parse,
    },
  ) {
    super(options, utils)
  }

  async init() {
    const {
      moduleName,
      amqpConnectionString,
      publishExchangeName,
      isTestMode,
      failedMessagesQueueName,
    } = this.options

    this.queueName = moduleName
    this.responseQueueName = `${moduleName}-response-${this.utils.newId()}`

    this.deadLetterQueueName =
      failedMessagesQueueName || `${moduleName}-failed-messages`

    const connection = await amqp.connect(amqpConnectionString)

    this.channel = await connection.openChannel()

    await this.channel.declareExchange({
      exchange: publishExchangeName,
      type: 'topic',
      durable: true,
    })

    await this.channel.declareQueue({
      queue: this.queueName,
      durable: true,
      exclusive: isTestMode ? false : undefined,
    })

    await this.channel.declareQueue({
      queue: this.responseQueueName,
      durable: true,
      exclusive: isTestMode ? false : true,
    })

    await this.channel.declareQueue({
      queue: this.deadLetterQueueName,
      durable: true,
      exclusive: isTestMode ? false : undefined,
    })
  }

  async start() {
    await super.start()

    const routes = this.getRegisteredRoutes()
    const prefixes = this.getRegisteredPrefixes()

    const patterns = prefixes.map(x => `${x}#`).concat(routes)

    // this.bindingSetup = async (x: ConfirmChannel) => {
    await Promise.all(
      patterns.map(pattern =>
        this.channel.bindQueue({
          queue: this.queueName,
          exchange: this.options.publishExchangeName,
          routingKey: pattern,
        }),
      ),
    )

    this.channel.consume(
      { queue: this.queueName },
      async (args, properties, data) => {
        if (!data.length) {
          return
        }
        const message: string = new TextDecoder().decode(data)
        const route = args.routingKey
        const replyTo = properties.replyTo
        const correlationId = properties.correlationId
        try {
          await this.processMessage({
            route,
            correlationId,
            message,
            replyTo,
          })
        } catch (err: any) {
          const fullMessage = this.utils.jsonDecode(message)
          await this.channel.publish(
            {
              routingKey: this.deadLetterQueueName,
            },
            {
              correlationId,
              replyTo,
              headers: {
                route,
              },
            },
            new TextEncoder().encode(
              this.utils.jsonEncode(<any>{
                ...fullMessage,
                handlingErrorData: normalizeError(err),
              }),
            ),
          )
        }
        this.channel.ack({ deliveryTag: args.deliveryTag })
      },
    )

    this.channel.consume(
      { queue: 'my.queue' },
      async (args, properties, data) => {
        if (!data.length) {
          return
        }

        const message: string = new TextDecoder().decode(data)

        const correlationId = properties.correlationId

        await this.channel.ack({ deliveryTag: args.deliveryTag })

        try {
          this.processResponseMessage({
            correlationId,
            message,
            route: args.routingKey,
          })
        } catch (err) {
          console.log('processResponseMessage.error', err)
        }
      },
    )
  }

  async stop() {
    await this.channel.close()

    await super.stop()
  }

  async dispose() {
    await super.dispose()

    await this.channel.close()
  }

  protected async sendMessage(props: SendMessageProps) {
    const { route, message, correlationId, isRpc } = props

    await this.channel.publish(
      {
        exchange: this.options.publishExchangeName,
        routingKey: route,
      },
      {
        ...(isRpc
          ? {
              replyTo: this.responseQueueName,
              correlationId,
            }
          : null),
      },
      new TextEncoder().encode(message),
    )
  }

  protected async sendReplyMessage(
    props: SendReplyMessageProps,
  ): Promise<void> {
    const { replyTo, correlationId, message } = props

    await this.channel.publish(
      {
        routingKey: replyTo,
      },
      {
        correlationId,
      },
      new TextEncoder().encode(message),
    )
  }
}
