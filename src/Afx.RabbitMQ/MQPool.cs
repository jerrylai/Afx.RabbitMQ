using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;


namespace Afx.RabbitMQ
{
    /// <summary>
    /// mq 应用池
    /// </summary>
    public class MQPool : IMQPool
    {
        private IConnectionFactory connectionFactory;
        private IConnection connection;
        private string clientName { get; set; }
        private IJsonSerialize jsonSerialize;

        private IChannel subChannel;
        private List<ConsumerBase> consumerList = new List<ConsumerBase>();

        private readonly int maxPool = 3;
        private ConcurrentQueue<IChannel> publishChannelQueue = new ConcurrentQueue<IChannel>();


        /// <summary>
        /// Returns true if the connection is still in a state where it can be used. Identical
        /// to checking if RabbitMQ.Client.IConnection.CloseReason equal null.
        /// </summary>
        public bool IsOpen { get { return this.connection?.IsOpen ?? false; } }
        /// <summary>
        /// The current heartbeat setting for this connection (System.TimeSpan.Zero for disabled).
        /// </summary>
        public TimeSpan Heartbeat { get { return this.connection?.Heartbeat ?? TimeSpan.Zero; } }

        /// <summary>
        /// 异常回调
        /// </summary>
        public Func<Exception, IDictionary<string, object>, string, Task> CallbackException;

        /// <summary>
        /// mq应用池
        /// </summary>
        /// <param name="hostName">mq服务器</param>
        /// <param name="port">mq端口</param>
        /// <param name="userName">登录账号</param>
        /// <param name="password">密码</param>
        /// <param name="jsonSerialize">json Serialize</param>
        /// <param name="virtualHost"></param>
        /// <param name="maxPool">push池大小</param>
        /// <param name="networkRecoveryInterval"></param>
        /// <param name="clientName"></param>
        public MQPool(string hostName, int port, string userName, string password, IJsonSerialize jsonSerialize, string virtualHost = "/", int maxPool = 3, int networkRecoveryInterval = 15, string clientName = null)
        {
            if (string.IsNullOrEmpty(hostName)) throw new ArgumentNullException(nameof(virtualHost));
            if (port <= System.Net.IPEndPoint.MinPort || System.Net.IPEndPoint.MaxPort <= port) throw new ArgumentException(nameof(port));
            if (string.IsNullOrEmpty(userName)) throw new ArgumentNullException(nameof(userName));
            if (jsonSerialize == null) throw new ArgumentNullException(nameof(jsonSerialize));
            if (virtualHost == null) throw new ArgumentNullException(nameof(virtualHost));
            if (maxPool < 0) throw new ArgumentException(nameof(maxPool));
            if (networkRecoveryInterval <= 0) throw new ArgumentException(nameof(networkRecoveryInterval));
            this.jsonSerialize = jsonSerialize;
            this.clientName = clientName ?? "Afx.RabbitMQ";
            this.connectionFactory = new ConnectionFactory()
            {
                HostName = hostName,
                Port = port,
                UserName = userName,
                Password = password,
                VirtualHost = virtualHost,
                AutomaticRecoveryEnabled = true,
                TopologyRecoveryEnabled = true,
                RequestedHeartbeat = TimeSpan.FromSeconds(30),
                NetworkRecoveryInterval = TimeSpan.FromSeconds(networkRecoveryInterval)
            };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public virtual async Task Open()
        {
            if (connection != null) return;

            connection = await this.connectionFactory.CreateConnectionAsync(this.clientName);
            connection.CallbackExceptionAsync += conCallbackException;
        }

        private async Task conCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            if (CallbackException != null)
                await CallbackException(e.Exception, e.Detail, string.Empty);
        }

        private async Task<PublishChannel> GetPublishChannel()
        {
            IChannel ch = null;
            while (this.publishChannelQueue.TryDequeue(out ch) && !ch.IsOpen)
            {
                ch.Dispose();
                ch = null;
            }
            if (ch == null)
            {
                if (this.connection == null) await this.Open();
                ch = await connection.CreateChannelAsync();
            }

            return new PublishChannel(this, ch);
        }

        #region Exchange
        /// <summary>
        /// 
        /// </summary>
        /// <param name="exchange"></param>
        /// <param name="type"></param>
        /// <param name="durable">是否持久化, 默认true</param>
        /// <param name="autoDelete">当已经没有消费者时，服务器是否可以删除该Exchange, 默认false</param>
        /// <param name="arguments"></param>
        public virtual async Task ExchangeDeclare(string exchange = "amq.direct", string type = "direct", bool durable = true, bool autoDelete = false, IDictionary<string, object> arguments = null)
        {
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            if (string.IsNullOrEmpty(type)) throw new ArgumentNullException(nameof(type));
            await using (var ph = await GetPublishChannel())
            {
                await ph.Channel.ExchangeDeclareAsync(exchange, type, durable, autoDelete, arguments);
            }
        }

        /// <summary>
        /// ExchangeDeclare
        /// </summary>
        /// <param name="config"></param>
        public virtual async Task ExchangeDeclare(ExchangeConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            await this.ExchangeDeclare(config.Exchange, config.Type, config.Durable, config.AutoDelete, config.Arguments);
        }

        /// <summary>
        /// 批量ExchangeDeclare
        /// </summary>
        /// <param name="configs"></param>
        public virtual async Task ExchangeDeclare(IEnumerable<ExchangeConfig> configs)
        {
            if (configs == null) throw new ArgumentNullException(nameof(configs));
            foreach (var item in configs)
            {
                if (item == null) throw new ArgumentNullException($"{nameof(configs)} item is null!");
                if (string.IsNullOrEmpty(item.Exchange)) throw new ArgumentNullException($"{nameof(configs)} item.{nameof(item.Exchange)} is null!");
                if (string.IsNullOrEmpty(item.Type)) throw new ArgumentNullException($"{nameof(configs)} item.{nameof(item.Type)} is null!");
            }
            await using (var ph = await GetPublishChannel())
            {
                foreach (var item in configs)
                {
                    await ph.Channel.ExchangeDeclareAsync(item.Exchange, item.Type, item.Durable, item.AutoDelete, item.Arguments);
                }
            }
        }

        /// <summary>
        /// QueueDeclare
        /// </summary>
        /// <param name="config"></param>
        public virtual async Task QueueDeclare(QueueConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (string.IsNullOrEmpty(config.Queue)) throw new ArgumentNullException(nameof(config.Queue));
            if (string.IsNullOrEmpty(config.Exchange)) throw new ArgumentNullException(nameof(config.Exchange));
            await using (var ph = await GetPublishChannel())
            {
                await ph.Channel.QueueDeclareAsync(config.Queue, config.Durable, config.Exclusive, config.AutoDelete, config.QueueArguments);
                await ph.Channel.QueueBindAsync(config.Queue, config.Exchange, config.RoutingKey ?? string.Empty, config.BindArguments);
                if (!string.IsNullOrEmpty(config.DelayQueue) && config.Queue != config.DelayQueue
                    && (config.RoutingKey != config.DelayRoutingKey || (string.IsNullOrEmpty(config.DelayRoutingKey) && string.IsNullOrEmpty(config.RoutingKey))))
                {
                    Dictionary<string, object> dic = new Dictionary<string, object>(2);
                    dic.Add("x-dead-letter-exchange", config.Exchange);
                    dic.Add("x-dead-letter-routing-key", config.RoutingKey ?? string.Empty);
                    await ph.Channel.QueueDeclareAsync(config.DelayQueue, config.Durable, config.Exclusive, config.AutoDelete, dic);
                    await ph.Channel.QueueBindAsync(config.DelayQueue, config.Exchange, config.DelayRoutingKey ?? string.Empty, null);
                }
            }
        }

        /// <summary>
        /// 批量QueueDeclare
        /// </summary>
        /// <param name="queues"></param>
        public virtual async Task QueueDeclare(IEnumerable<QueueConfig> queues)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            foreach (var item in queues)
            {
                if (item == null) throw new ArgumentNullException($"{nameof(queues)} item is null!");
                if (string.IsNullOrEmpty(item.Queue)) throw new ArgumentNullException($"{nameof(queues)} item.{nameof(item.Queue)} is null!");
                if (string.IsNullOrEmpty(item.Exchange)) throw new ArgumentNullException($"{nameof(queues)} item.{nameof(item.Exchange)} is null!");
            }
            await using (var ph = await GetPublishChannel())
            {
                foreach (var config in queues)
                {
                    var ok = await ph.Channel.QueueDeclareAsync(config.Queue, config.Durable, config.Exclusive, config.AutoDelete, config.QueueArguments);
                    await ph.Channel.QueueBindAsync(config.Queue, config.Exchange, config.RoutingKey ?? string.Empty, config.BindArguments);
                    if (!string.IsNullOrEmpty(config.DelayQueue) && config.Queue != config.DelayQueue
                    && (config.RoutingKey != config.DelayRoutingKey || (string.IsNullOrEmpty(config.DelayRoutingKey) && string.IsNullOrEmpty(config.RoutingKey))))
                    {
                        Dictionary<string, object> dic = new Dictionary<string, object>(2);
                        dic.Add("x-dead-letter-exchange", config.Exchange);
                        dic.Add("x-dead-letter-routing-key", config.RoutingKey ?? string.Empty);
                        ok = await ph.Channel.QueueDeclareAsync(config.DelayQueue, config.Durable, config.Exclusive, config.AutoDelete, dic);
                        await ph.Channel.QueueBindAsync(config.DelayQueue, config.Exchange, config.DelayRoutingKey ?? string.Empty, null);
                    }
                }
            }
        }

        #endregion

        #region Publish
        private ReadOnlyMemory<byte> Serialize<T>(T m, out string contentType)
        {
            contentType = null;
            ReadOnlyMemory<byte> result = null;
            if (m is ReadOnlyMemory<byte>)
            {
                contentType = "application/octet-stream";
                object o = m;
                result = (ReadOnlyMemory<byte>)o;
            }
            else if (m is byte[])
            {
                contentType = "application/octet-stream";
                object o = m;
                result = (o as byte[]);
            }
            else if (m is string)
            {
                contentType = "text/plain";
                object o = m;
                result = Encoding.UTF8.GetBytes(o as string);
            }
            else
            {
                var json = this.jsonSerialize.Serialize(m);
                contentType = "application/json";
                result = Encoding.UTF8.GetBytes(json);
            }

            return result;
        }

        /// <summary>
        /// 发布消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg">消息</param>
        /// <param name="routingKey">routingKey</param>
        /// <param name="expire">消息过期时间</param>
        /// <param name="exchange">exchange</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="headers">headers</param>
        /// <param name="mandatory">如果设置为 true，表示消息必须成功路由到一个队列，否则会将消息退回给生产者。</param>
        /// <returns>是否发生成功</returns>
        public virtual async Task<bool> Publish<T>(T msg, string routingKey, TimeSpan? expire = null, string exchange = "amq.direct", bool persistent = false,
            IDictionary<string, object> headers = null, bool mandatory = false)
        {
            if (msg == null) throw new ArgumentNullException(nameof(msg));
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            if (expire.HasValue && expire.Value.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(expire)}({expire}) is error!");
            var body = Serialize<T>(msg, out var contentType);
            await using (var ph = await GetPublishChannel())
            {
                var props = new BasicProperties();
                props.Persistent = persistent;
                props.ContentType = contentType;
                props.ContentEncoding = "utf-8";
                if (expire.HasValue) props.Expiration = expire.Value.TotalMilliseconds.ToString("f0");
                if (headers != null) foreach (KeyValuePair<string, object> kv in headers) props.Headers[kv.Key] = kv.Value;
                await ph.Channel.BasicPublishAsync(exchange, routingKey ?? string.Empty, mandatory, props, body);
            }

            return true;
        }

        /// <summary>
        /// 发布消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="expire">消息过期时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="headers">headers</param>
        /// <param name="mandatory">如果设置为 true，表示消息必须成功路由到一个队列，否则会将消息退回给生产者。</param>
        /// <returns></returns>
        public virtual async Task<bool> Publish<T>(T msg, PubConfig config, TimeSpan? expire = null, bool persistent = false,
            IDictionary<string, object> headers = null, bool mandatory = false)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            return await this.Publish(msg, config.RoutingKey, expire, config.Exchange, persistent, headers, mandatory);
        }

        /// <summary>
        /// 批量发布消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msgs">消息</param>
        /// <param name="routingKey">routingKey</param>
        /// <param name="expire">消息过期时间</param>
        /// <param name="exchange">exchange</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="headers">headers</param>
        /// <param name="mandatory">如果设置为 true，表示消息必须成功路由到一个队列，否则会将消息退回给生产者。</param>
        /// <returns>是否发生成功</returns>
        public virtual async Task<bool> Publish<T>(List<T> msgs, string routingKey, TimeSpan? expire = null, string exchange = "amq.direct", bool persistent = false,
            IDictionary<string, object> headers = null, bool mandatory = false)
        {
            if (msgs == null) throw new ArgumentNullException(nameof(msgs));
            if (msgs.Count == 0) return true;
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            if (expire.HasValue && expire.Value.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(expire)}({expire}) is error!");
            await using (var ph = await GetPublishChannel())
            {
                var props = new BasicProperties();
                props.Persistent = persistent;
                props.ContentEncoding = "utf-8";
                if (expire.HasValue) props.Expiration = expire.Value.TotalMilliseconds.ToString("f0");
                if (headers != null) foreach (KeyValuePair<string, object> kv in headers) props.Headers[kv.Key] = kv.Value;

                foreach (var m in msgs)
                {
                    var body = Serialize<T>(m, out var contentType);
                    props.ContentType = contentType;
                    await ph.Channel.BasicPublishAsync(exchange, routingKey ?? string.Empty, mandatory, props, body);
                }
            }
            return true;
        }

        /// <summary>
        /// 发布消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msgs">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="expire">消息过期时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="headers">headers</param>
        /// <param name="mandatory">如果设置为 true，表示消息必须成功路由到一个队列，否则会将消息退回给生产者。</param>
        /// <returns></returns>
        public virtual async Task<bool> Publish<T>(List<T> msgs, PubConfig config, TimeSpan? expire = null, bool persistent = false,
            IDictionary<string, object> headers = null, bool mandatory = false)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            return await this.Publish(msgs, config.RoutingKey, expire, config.Exchange, persistent, headers, mandatory);
        }

        /// <summary>
        /// 发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg">消息</param>
        /// <param name="delayRoutingKey">delayRoutingKey</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="exchange">exchange</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="headers">headers</param>
        /// <param name="mandatory">如果设置为 true，表示消息必须成功路由到一个队列，否则会将消息退回给生产者。</param>
        /// <returns>是否发生成功</returns>
        public virtual async Task<bool> PublishDelay<T>(T msg, string delayRoutingKey, TimeSpan delay, string exchange = "amq.direct", bool persistent = false,
            IDictionary<string, object> headers = null, bool mandatory = false)
        {
            if (delay.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(delay)} is error!");

            return await this.Publish(msg, delayRoutingKey, delay, exchange, persistent, headers, mandatory);
        }

        /// <summary>
        /// 发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="headers">headers</param>
        /// <param name="mandatory">如果设置为 true，表示消息必须成功路由到一个队列，否则会将消息退回给生产者。</param>
        /// <returns>是否发生成功</returns>
        public virtual async Task<bool> PublishDelay<T>(T msg, PubConfig config, TimeSpan delay, bool persistent = false,
            IDictionary<string, object> headers = null, bool mandatory = false)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));

            return await this.Publish(msg, config.DelayRoutingKey, delay, config.Exchange, persistent, headers, mandatory);
        }

        /// <summary>
        /// 批量发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msgs">消息</param>
        /// <param name="delayRoutingKey">delayRoutingKey</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="exchange">exchange</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="headers">headers</param>
        /// <param name="mandatory">如果设置为 true，表示消息必须成功路由到一个队列，否则会将消息退回给生产者。</param>
        /// <returns>是否发生成功</returns>
        public virtual async Task<bool> PublishDelay<T>(List<T> msgs, string delayRoutingKey, TimeSpan delay, string exchange = "amq.direct", bool persistent = false,
            IDictionary<string, object> headers = null, bool mandatory = false)
        {
            if (msgs == null) throw new ArgumentNullException(nameof(msgs));
            if (msgs.Count == 0) return true;
            if (delay.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(delay)} is error!");

            return await this.Publish(msgs, delayRoutingKey, delay, exchange, persistent, headers, mandatory);
        }

        /// <summary>
        /// 发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msgs">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="headers">headers</param>
        /// <param name="mandatory">如果设置为 true，表示消息必须成功路由到一个队列，否则会将消息退回给生产者。</param>
        /// <returns>是否发生成功</returns>
        public virtual async Task<bool> PublishDelay<T>(List<T> msgs, PubConfig config, TimeSpan delay, bool persistent = false,
            IDictionary<string, object> headers = null, bool mandatory = false)
        {
            if (msgs == null) throw new ArgumentNullException(nameof(msgs));
            if (msgs.Count == 0) return true;
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (delay.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(delay)} is error!");

            return await this.Publish(msgs, config.DelayRoutingKey, delay, config.Exchange, persistent, headers, mandatory);
        }

        #endregion

        private T Deserialize<T>(ReadOnlyMemory<byte> buffer)
        {
            T result = default(T);
            var t = typeof(T);
            if (buffer is T)
            {
                object o = buffer;
                result = (T)o;
            }
            else if (t == typeof(byte[]))
            {
                object o = buffer.ToArray();
                result = (T)o;
            }
            else if (t == typeof(string))
            {
                object o = Encoding.UTF8.GetString(buffer.ToArray());
                result = (T)o;
            }
            else
            {
                var json = Encoding.UTF8.GetString(buffer.ToArray());
                try
                {
                    result = this.jsonSerialize.Deserialize<T>(json);
                }
                catch (Exception ex)
                {
                    if (this.CallbackException != null)
                        this.CallbackException.Invoke(ex, null, $"{typeof(T).FullName}, json: {json}");
                }
            }

            return result;
        }

        /// <summary>
        /// 消费消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="hander"></param>
        /// <param name="queue"></param>
        /// <param name="autoAck">是否自动确认</param>
        /// <param name="newTask">是否使用新线程</param>
        public virtual async Task Subscribe<T>(AsyncSubscribeHander<T> hander, string queue, bool autoAck = false, bool newTask = false)
        {
            if (hander == null) throw new ArgumentNullException(nameof(hander));
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            if (this.subChannel == null)
            {
                if (this.connection == null) await this.Open();
                this.subChannel = await this.connection.CreateChannelAsync();
            }
            await subChannel.BasicQosAsync(0, 1, false);
            var eventingBasicConsumer = new AsyncEventingBasicConsumer(this.subChannel);
            var consumer = new AsyncConsumer<T>(this, eventingBasicConsumer, hander, autoAck, newTask);
            this.consumerList.Add(consumer);
            eventingBasicConsumer.ReceivedAsync += consumer.Handler;
            await this.subChannel.BasicConsumeAsync(queue, autoAck, eventingBasicConsumer);
        }

        /// <summary>
        /// 
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            await this.DisposeAsync(true);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual async ValueTask DisposeAsync(bool disposing)
        {
            if (disposing)
            {
                if (this.subChannel != null)
                {
                    try { if (this.subChannel.IsOpen) await this.subChannel.CloseAsync(); } catch { }
                    try { await this.subChannel.DisposeAsync(); } catch { }
                }
                this.subChannel = null;
                IChannel model;
                while (this.publishChannelQueue != null && this.publishChannelQueue.TryDequeue(out model))
                {
                    if (model != null)
                    {
                        try { if (model.IsOpen) await model.CloseAsync(); } catch { }
                        try { await model.DisposeAsync(); } catch { }
                    }
                }
                this.publishChannelQueue = null;
                if (this.connection != null)
                {
                    try { if (this.connection.IsOpen) await this.connection.CloseAsync(); } catch { }
                    try { await this.connection.DisposeAsync(); } catch { }
                }
                this.connection = null;
                this.CallbackException = null;

                if (this.consumerList != null)
                {
                    foreach (var c in this.consumerList) await c.DisposeAsync();
                    this.consumerList.Clear();
                    this.consumerList.TrimExcess();
                }
                this.consumerList = null;
            }
        }

        class PublishChannel : IAsyncDisposable
        {
            private MQPool pool;
            public IChannel Channel { get; private set; }
            public PublishChannel(MQPool pool, IChannel channel)
            {
                if (pool == null) throw new ArgumentNullException(nameof(pool));
                if (channel == null) throw new ArgumentNullException(nameof(channel));
                this.pool = pool;
                this.Channel = channel;
            }

            public async ValueTask DisposeAsync()
            {
                if (this.pool != null)
                {
                    if (this.pool.maxPool > this.pool.publishChannelQueue.Count && this.Channel.IsOpen)
                    {
                        this.pool.publishChannelQueue.Enqueue(this.Channel);
                    }
                    else
                    {
                        if (this.Channel.IsOpen) await this.Channel.CloseAsync();
                        await this.Channel.DisposeAsync();
                    }
                }
                this.pool = null;
                this.Channel = null;
            }
        }

        class ConsumerBase : IAsyncDisposable
        {
            protected MQPool pool;

            public ConsumerBase(MQPool pool)
            {
                this.pool = pool;
            }

            public virtual ValueTask DisposeAsync(bool disposable)
            {
                if (disposable)
                {
                    this.pool = null;
                }

                return ValueTask.CompletedTask;
            }

            public async ValueTask DisposeAsync()
            {
                await this.DisposeAsync(true);
            }
        }

        class AsyncConsumer<T> : ConsumerBase
        {
            private AsyncEventingBasicConsumer consumer;
            private AsyncSubscribeHander<T> hander;
            private bool autoAck;
            private bool newTask;

            public AsyncConsumer(MQPool pool, AsyncEventingBasicConsumer consumer, AsyncSubscribeHander<T> hander, bool autoAck, bool newTask = false)
                : base(pool)
            {
                this.consumer = consumer;
                this.hander = hander;
                this.autoAck = autoAck;
                this.newTask = newTask;
            }

            public async Task Handler(object sender, BasicDeliverEventArgs e)
            {
                if (this.newTask)
                {
                    var t = Task.Factory.StartNew(this.Exec, e);
                }
                else
                {
                    await this.Exec(e);
                }
            }

            private async Task Exec(object o)
            {
                BasicDeliverEventArgs e = o as BasicDeliverEventArgs;
                bool handerOk = false;
                try
                {
                    T m = this.pool.Deserialize<T>(e.Body);
                    if (m != null) handerOk = await hander(m, e.BasicProperties?.Headers);
                    else handerOk = true;
                }
                catch (Exception ex)
                {
                    try { this.pool.CallbackException?.Invoke(ex, null, string.Empty); }
                    catch { }
                }

                if (!this.autoAck)
                {
                    if (handerOk)
                    {
                        await consumer.Channel.BasicAckAsync(e.DeliveryTag, false);
                    }
                    else
                    {
                        await consumer.Channel.BasicNackAsync(e.DeliveryTag, false, true);
                    }
                }
            }

            public override async ValueTask DisposeAsync(bool disposable)
            {
                if (disposable)
                {
                    this.consumer = null;
                    this.hander = null;
                }
                await base.DisposeAsync(disposable);
            }
        }
    }
}