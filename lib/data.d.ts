declare class Message {
  storeSize: number
  bodyCRC: number
  queueId: number
  flag: number
  queueOffset: number
  commitLogOffset: number
  bornHost: string
  bornTimestamp: number
  storeTimestamp: number
  storeHost: string
  reconsumeTimes: number
  preparedTransactionOffset: number
  topic: string
  properties: { [key: string]: any }
  msgId: string
  body: string | Buffer
  //
  tags: string
  keys: string
  originMessageId: string
  retryTopic: string
  delayTimeLevel: number
  waitStoreMsgOK: boolean
  buyerId: string
  setStartDeliverTime (delayTime: number): void
  getStartDeliverTime (): number
}

declare class MessageQueue {
  readonly topic: string
  readonly brokerName: string
  readonly queueId: number
  readonly key: string
  toJSON (): any
  static fromJSON (json: any): MessageQueue
}

declare class ProcessQueue {
  msgList: Message[]
  consumingMsgOrderlyList: Message[]
  dropped: boolean
  lastPullTimestamp: number
  lastConsumeTimestamp: number

  locked: boolean
  lastLockTimestamp: number
  tryUnlockTimes: number
  readonly msgSize: number
  readonly lockConsume: Lock

  readonly maxSpan: number
  readonly msgCount: number

  readonly isPullExpired: boolean
  readonly lockExpired: boolean

  putMessage (msgs: Message[]): void
  remove (msg: Message): number
  clear (): void
  cleanExpiredMsg (pushConsumer: MQPushConsumer): Promise<void>
  takeMessage (): Message
  commit (): void
  makeMessageToCosumeAgain (msg: Message): void
  fillProcessQueueInfo (info: any): void
  incTryUnlockTimes (): void
  hasTempMessage (): boolean

  static REBALANCE_LOCK_INTERVAL: number
}

declare interface Lock {
  lock(): Promise<void>
  tryLock(timeout: number): Promise<boolean>
  unlock(): void
}

declare interface ConsumeMessageService {
  init(): void
  close(): Promise<void>
  consumeMessageDirectly(
    msg: Message,
    brokerName: string
  ): Promise<ConsumeMessageDirectlyResult>
  submitConsumeRequest(
    msg: Message,
    processQueue: ProcessQueue,
    messageQueue: MessageQueue
  ): Promise<void>
}

declare interface ConsumeMessageConcurrentlyService
  extends ConsumeMessageService {
  resetRetryTopic(msg: Message): void
}

declare interface ConsumeMessageOrderlyService extends ConsumeMessageService {
  unlockAllMQ(): Promise<void>
  lockMQPeriodically(): Promise<void>
  lockOneMQ(messageQueue: MessageQueue): Promise<boolean>
}

declare interface ConsumeMessageDirectlyResult {
  order: false
  autoCommit: true
  consumeResult: null
  remark: null
  spentTimeMills: 0
}

declare interface OffsetStore {
  load(): Promise<void>
  updateOffset(
    messageQueue: MessageQueue,
    offset: number,
    increaseOnly: boolean
  ): void
  readOffset(messageQueue: MessageQueue, type: string): Promise<void>
  persistAll(mqs: MessageQueue[]): Promise<void>
  persist(messageQueue: MessageQueue): Promise<void>
  removeOffset(messageQueue: MessageQueue): void
  fetchConsumeOffsetFromBroker(messageQueue: MessageQueue): Promise<number>
  updateConsumeOffsetToBroker(
    messageQueue: MessageQueue,
    offset: number
  ): Promise<void>
  findBrokerAddressInAdmin(messageQueue: MessageQueue): any
}

declare interface CommandOption {
  /**
   * 命令代号
   */
  code: number
  /**
   * 自定义头部
   */
  cuustomHeader: any
  /**
   * 标识命令的属性
   */
  flag: number
  /**
   * 关联请求、响应的字段
   */
  opaque: number
  remark: string
  /**
   * 扩展字段
   */
  extFields: any
}

declare class RemotingCommand {
  constructor (options: CommandOption) {
    this.code = options.code
    this.customHeader = options.customHeader
    this.flag = options.flag || 0
    this.opaque = options.opaque || OpaqueGenerator.getNextOpaque()
    this.remark = options.remark
    this.extFields = options.extFields || {}
    this.body = null
  }

  readonly language: string
  readonly version: number
  readonly type: string

  readonly isResponseType: boolean
  readonly isOnewayRPC: boolean
  markResponseType (): void
  markOnewayRPC (): void
  makeCustomHeaderToNet (): void

  decodeCommandCustomHeader (): any
  buildHeader (): void
  encode (): Buffer

  /**
   * 创建 request 命令
   * @param code - 命令代号
   * @param customHeader -
   * @return command
   */
  static createRequestCommand (code: number, customHeader: any): RemotingCommand

  /**
   * 创建 request 命令
   * @param code - 命令代号
   * @param opaque -
   * @param remark -
   * @return command
   */
  static createResponseCommand (
    code: number,
    opaque: number,
    remark: string
  ): RemotingCommand

  /**
   * 反序列化报文
   * @param packet -
   * @return  command
   */
  static decode (packet: Buffer): RemotingCommand
}
