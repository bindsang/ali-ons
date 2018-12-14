@mctech/ali-ons
=======

fork from ali-sdk/ali-ons

## 增加的功能

1. 发送消息的时候增加了sharding功能

```javascript
const sendResult = await producer.send(msg, shardingKey);
```

2. 同时支持ons和rocketmq

ons连接方式与官方版本一致
rocketmq连接方式如下：

```javascript
const producer = new Producer({
  ...
  namesrvAddr: '192.168.1.1' // namesrv的地址，格式为ip[[:port],ip[:port],...]
})
const consumer = new Consumer({
  ...
  namesrvAddr: '192.168.1.1' // namesrv的地址，格式为ip[[:port],ip[:port],...]
})
```
3. 部分功能移植自java版本
   1. 实现了有序消息和无序消息的消费，配合sharding功能可实现全局有序消息和分片有序的消息
   1. 实现了有序消息和无序消息的ack功能。官方版本没有实现这个重要功能，导致消费出错后消息就丢失了
   1. 实现了处理来自broker的命令，目前支持 `NOTIFY_CONSUMER_IDS_CHANGED`, `RESET_CONSUMER_CLIENT_OFFSET`, `GET_CONSUMER_RUNNING_INFO`, `GET_CONSUMER_STATUS_FROM_CLIENT`, `CONSUME_MESSAGE_DIRECTLY`几个命令。
   通过上述命令可在管理控制台获取consumer的详细信息，重置消费队列的位置
   1. consumer增加了suspend和resume两个方法
4. 优化了consumer增加节点与减少节点时消息被重复消费的概率。
   连续发送2000条消息，同时假定消费端基本不用时间。首先启动一个consumer消费到一小部分的时候再起另一个consumer。使用官司方版本的话，第一个节点上大概会消费1300多条消息（根据后一个节点起动的时间间隔，略有偏差），后一个节点一定会消费1000条消息。这样总共消费的条数就是2300多，重复消费的消息有300多条。
   优化后的版本在同样的条件下绝大部分情况都不会重复消费，只在极个别情况下会多消费一条，两条左右。这个是由于rocketmq本身的原理决定了的，基本上无法避免
4. 修改了官方版本在极端情况下会丢消息的bug。
   我们知道offsetStore对象中存放的是本地已提交的消息的偏移量(commitOffset)，后台有个定时器会定时把这个值刷新到broker中。
   当本地获取到的一批消息还没有消费完时，后台定时获取消息的代码从broker上获取不到消息的时候，官方版本会把本地offsetStore中的偏移量值设置为request返回的nextOffset值，下一轮定时刷新已提交的偏移量就会把这个值写到broker中，而不会管偏移量之前的消息是否真的已经消费完成。
   如果此时进程被终结了，这批在内存中还未消费的消息就永远丢失了
   相关代码参见'mq_push_consumer.js'的`correctTagsOffset`方法


## Usage

consumer

```js
'use strict';

const httpclient = require('urllib');
const Consumer = require('ali-ons').Consumer;
const consumer = new Consumer({
  httpclient,
  accessKeyId: 'your-accessKeyId',
  accessKeySecret: 'your-AccessKeySecret',
  consumerGroup: 'your-consumer-group',
  // isBroadcast: true,
});

consumer.subscribe(config.topic, '*', async msg => {
  console.log(`receive message, msgId: ${msg.msgId}, body: ${msg.body.toString()}`)
});

// 有序消息
const orderlyListener = async msg => {
  console.log(`receive message, msgId: ${msg.msgId}, body: ${msg.body.toString()}`)
}
orderlyListener.isOrder = true
consumer.subscribe(config.topic, '*', orderlyListener);

consumer.on('error', err => console.log(err));
```

producer

```js
'use strict';
const httpclient = require('urllib');
const Producer = require('ali-ons').Producer;
const Message = require('ali-ons').Message;

const producer = new Producer({
  httpclient,
  accessKeyId: 'your-accessKeyId',
  accessKeySecret: 'your-AccessKeySecret',
  producerGroup: 'your-producer-group',
});

(async () => {
  const msg = new Message('your-topic', // topic
    'TagA', // tag
    'Hello ONS !!! ' // body
  );

  // set Message#keys
  msg.keys = ['key1'];

  // 不分片，随机投放消息
  const sendResult1 = await producer.send(msg);
  // 指定分片字符串参数，根据hash值计算后的续果投放
  const shardingKey = getShardingKey() // string
  const sendResult2 = await producer.send(msg, shardingKey);
  console.log(sendResult);
})().catch(err => console.error(err))
```

rocketmq

使用下面的类替换httpclient

由于内部校验的原因，在连接rocketmq服务器的模式下，onsAddr属性即使没有用到也不能为空值，可随便指定一个值

```js
class HttpClientProxy {
  constructor (option) {
    this.namesrvAddr = null
    if (option.namesrvAddr) {
      this.namesrvAddr = { data: option.namesrvAddr, status: 200 }
    }
  }

  async request (url, option) {
    if (this.namesrvAddr) {
      // 设置了namesrvAddr属性，表示使用私有部署的rocketmq消息队列服务
      return this.namesrvAddr
    }

    try {
      const srvAddr = await new Promise((resolve, reject) => {
        let req = http.get(url, res => {
          // Read result
          res.setEncoding('utf8')
          let response = ''
          res
            .on('error', e => {
              return reject(e)
            })
            .on('data', chunk => {
              response += chunk
              if (response.length > 1e6) {
                req.connection.destroy()
              }
            })
            .on('end', () => {
              return resolve(response)
            })
        })
        // Connection error with the CAS server
        req.on('error', function (err) {
          req.abort()
          return reject(err)
        })
      })
      return { data: srvAddr, status: 200 }
    } catch (err) {
      return { status: 500 }
    }
  }
}

const namesrvAddr = '...'
const onsAddr = 'http://no_use'

const producer = new Producer({
  httpclient: new HttpClientProxy({ namesrvAddr }),
  accessKey,
  secretKey,
  producerGroup: producerId,
  onsAddr
})


const consumer = new Consumer({
  httpclient: new HttpClientProxy({ namesrvAddr }),
  accessKey,
  secretKey,
  consumerGroup: consumerId,
  onsAddr
})

```
## License

[MIT](LICENSE)
