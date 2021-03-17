import {
  nats,
  TransportBase,
  TransportOptions,
  TransportUtils,
  SendMessageProps,
  SendReplyMessageProps,
} from '../deps.ts'

// Credits: Kyle Bjordahl

export class NatsTransport extends TransportBase {
  protected nc: nats.NatsConnection | null = null
  protected subscriptions: nats.Subscription[] = []
  protected correlationIdHeader = 'X-CHEEP-CORRELATION-ID'

  private codec: nats.Codec<string>

  constructor(
    protected options: TransportOptions & {
      moduleName: string
      /** array of urls to connect to, may optionally contain username and password or token encoded in the url */
      natsServerUrls?: string[] | string
      isTestMode?: boolean
      maxPingOut?: number

      /** optional authentication method with password */
      user?: string
      /** optional authentication method with user */
      password?: string

      /** optional authentication method */
      token?: string
    },
    protected utils: TransportUtils,
  ) {
    super(options, utils)

    this.codec = nats.StringCodec()
  }

  async init(): Promise<void> {
    this.nc = await nats.connect({
      servers: this.options.natsServerUrls,
      name: this.options.moduleName,
      maxPingOut: this.options.maxPingOut ?? 5,
      // we want to receive our own events, just in case
      noEcho: false,
      user: this.options.user,
      pass: this.options.password,
      token: this.options.token,
      reconnect: true,
    })
  }

  async start() {
    if (!this.nc || this.nc?.isClosed()) {
      await this.init()
    }
    // specific route handlers
    const routes = this.getRegisteredRoutes()
    // wildcard ending routes
    const prefixes = this.getRegisteredPrefixes()
    // everyone together!
    const patterns = prefixes
      // put the nats wildcard suffix on the prefix routes
      .map(x => `${x}.>`)
      // remove any routes that are already covered by prefixes
      // this avoids duplicate delivery
      .concat(
        routes.filter(r => !prefixes.find(p => r.startsWith(p))),
      )

    // console.log(`NATS subscriptions:`, patterns)

    this.subscriptions = patterns.map(p =>
      this.nc!.subscribe(p, {
        callback: async (err, msg) => {
          if (!msg) {
            return
          }

          const message = this.codec.decode(msg.data)

          const route = msg.subject
          const replyTo = msg.reply

          try {
            await this.processMessage({
              route,
              correlationId: undefined,
              message,
              replyTo,
            })
          } catch (err) {
            // TODO: dead letter queue
            // await this.channel.sendToQueue(
            //   this.deadLetterQueueName,
            //   msg.content,
            //   {
            //     correlationId,
            //     replyTo,
            //     CC: route,
            //   },
            // )
          }

          // TODO: ack!
        },
      }),
    )

    await super.start()
  }

  async stop() {
    if (!this.nc) {
      return
    }

    // ensure all messages have been sent
    await this.nc.flush()
    // ensure all messages in flight have been processed
    await this.nc.drain()

    await super.stop()
  }

  async dispose() {
    await this.stop()

    await super.dispose()
  }

  protected async sendMessage(
    props: SendMessageProps,
  ): Promise<void> {
    if (!this.nc) {
      return
    }

    if (props.isRpc) {
      const msg = await this.nc.request(
        props.route,
        this.codec.encode(props.message),
        {
          timeout: this.options.defaultRpcTimeout ?? 1000,
        },
      )

      const message: string = this.codec.decode(msg.data)

      this.processResponseMessage({
        correlationId: props.correlationId,
        message,
        route: msg.subject,
      })
    } else {
      this.nc.publish(props.route, this.codec.encode(props.message))

      await this.nc.flush()
    }
  }

  protected async sendReplyMessage(
    props: SendReplyMessageProps,
  ): Promise<void> {
    if (!this.nc) {
      return
    }

    this.nc.publish(
      props.replyTo,
      this.codec.encode(props.message),
      {},
    )

    await this.nc.flush()
  }
}
