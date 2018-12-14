export class Message {
  constructor(topic: string, body: string | Buffer)
  constructor(topic: string, tags: string, body: string | Buffer)

  /**
   * 消息标签，默认值为undefined。用于接收消息时过滤
   */
  tags: string
  /**
   * 消息关键词，默认值为undefined。可在ons或rocketmq控制台中查看到
   */
  keys: string
  /**
   * 消息延时投递时间级别，默认值为0。0表示不延时，大于0表示特定延时级别（具体级别在服务器端定义）
   */
  delayTimeLevel: string
  /**
   * 是否等待服务器将消息存储完毕再返回（可能是等待刷盘完成或者等待同步复制到其他服务器）
   */
  waitStoreMsgOK: boolean
  /**
   * 消费重试次数，初始值为0
   */
  reconsumeTimes: number
  topic: string
  properties: { [key: string]: string }
  msgId: string
  body: Buffer
}

interface MessageQueue {
  topic: string
  brokerName: string
  queueId: number
}

interface Logger {
  error(msg: any, ...args: any[]): void
  warn(msg: any, ...args: any[]): void
  info(msg: any, ...args: any[]): void
  debug(msg: any, ...args: any[]): void
}

interface Response {
  data: string
  status: number
}

interface HttpClientOption {
  timeout: number
}

interface HttpClient {
  request(url: string | URL, option: HttpClientOption): Promise<Response>
}

interface ProducerOptions {
  producerGroup: string
  httpclient: HttpClient
  logger?: Logger
  createTopicKey?: string
  /**
   * default 4
   */
  defaultTopicQueueNums?: number
  /**
   * default 3000
   */
  sendMsgTimeout?: number
  /**
   * default 1024 * 4
   */
  compressMsgBodyOverHowmuch?: number
  /**
   * default 3
   */
  retryTimesWhenSendFailed?: number
  /**
   * default false
   */
  retryAnotherBrokerWhenNotStoreOK?: boolean
  /**
   * default 1024 * 128
   */
  maxMessageSize?: number
  /**
   * default 10000 ms
   */
  connectTimeout?: number
}

interface OnsProducerOptions extends ProducerOptions {
  accessKey: string
  secretKey: string
  onsAddr: string
}

interface RocketMQProducerOptions extends ProducerOptions {
  namesrvAddr: string
}

export class Producer {
  /**
   *
   * @param options
   * @param autoInitial 是否自动调用init函数初始化，默认值为true
   */
  constructor(options: OnsProducerOptions | RocketMQProducerOptions, autoInitial?: boolean)

  readonly producerGroup: string
  /**
   * start the producer
   */
  init(startFactory?: boolean): Promise<void>
  /**
   * close the producer
   */
  close(): Promise<void>

  /**
   * send message
   * @param msg message object
   * @param shardingKey sharding key
   * @returns sendResult
   */
  send(msg: Message, shardingKey: string): Promise<SendResult>
}

type SendStatus =
  // 消息发送成功
  | 'SEND_OK'
  // 消息发送成功，但是服务器刷盘超时，消息已经进入服务器队列，只有此时服务器宕机，消息才会丢失
  | 'FLUSH_DISK_TIMEOUT'
  // 消息发送成功，但是服务器同步到Slave时超时，消息已经进入服务器队列，只有此时服务器宕机，消息才会丢失
  | 'FLUSH_SLAVE_TIMEOUT'
  //  消息发送成功，但是服务器同步到Slave时超时，消息已经进入服务器队列，只有此时服务器宕机，消息才会丢失: 'SLAVE_NOT_AVAILABLE',
  | 'SLAVE_NOT_AVAILABLE'

interface SendResult {
  sendStatus: SendStatus
  msgId: string
  messageQueue: MessageQueue
  queueOffset: number
}

type ConsumeOrderlyStatus = 'SUCCESS' | 'SUSPEND_CURRENT_QUEUE_A_MOMENT'

interface OrderlyMessageListener {
  (msg: Message, mq: MessageQueue): Promise<ConsumeOrderlyStatus>
  isOrder: true
}

type ConsumeConcurrentlyStatus = 'CONSUME_SUCCESS' | 'RECONSUME_LATER'

interface ConcurrentMessageListener {
  (msg: Message, mq: MessageQueue): Promise<ConsumeConcurrentlyStatus>
}

export class Consumer {
  constructor(options: ConsumerOptions)

  readonly isPause: boolean
  readonly consumerGroup: string
  init(): Promise<void>
  close(): Promise<void>
  subscribe(topic: string, handler: OrderlyListener | ConcurrentMessageListener): void
  subscribe(topic: string, subExpression: string, handler: OrderlyListener | ConcurrentMessageListener): void
}
