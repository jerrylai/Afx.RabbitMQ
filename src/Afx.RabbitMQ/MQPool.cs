using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;


namespace Afx.RabbitMQ
{
    /// <summary>
    /// mq 应用池
    /// </summary>
    public class MQPool : IMQPool
    {
        private object lockCreate = new object();
        private IAsyncConnectionFactory m_connectionFactory;
        private IConnection m_connection;
        private string clientName { get; set; }
        private IJsonSerialize jsonSerialize;

        private IModel m_subChannel;
        private object lockSubChannel = new object();
        private List<ConsumerBase> consumerList = new List<ConsumerBase>();

        private readonly int maxPool = 3;
        private ConcurrentQueue<IModel> m_publishChannelQueue = new ConcurrentQueue<IModel>();
      

        /// <summary>
        /// Returns true if the connection is still in a state where it can be used. Identical
        /// to checking if RabbitMQ.Client.IConnection.CloseReason equal null.
        /// </summary>
        public bool IsOpen { get { return this.m_connection?.IsOpen ?? false; } }
        /// <summary>
        /// The current heartbeat setting for this connection (System.TimeSpan.Zero for disabled).
        /// </summary>
        public TimeSpan Heartbeat { get { return  this.m_connection?.Heartbeat ?? TimeSpan.Zero; } }

        /// <summary>
        /// 异常回调
        /// </summary>
        public Action<Exception, IDictionary<string, object>, string> CallbackException;

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
        /// <param name="consumersAsync">async 消费</param>
        public MQPool(string hostName, int port, string userName, string password, IJsonSerialize jsonSerialize, string virtualHost = "/", int maxPool = 3, int networkRecoveryInterval = 15, string clientName = null, bool consumersAsync = true)
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
            this.m_connectionFactory = new ConnectionFactory()
            {
                HostName = hostName,
                Port = port,
                UserName = userName,
                Password = password,
                VirtualHost = virtualHost,
                DispatchConsumersAsync = consumersAsync,
                AutomaticRecoveryEnabled = true,
                NetworkRecoveryInterval = TimeSpan.FromSeconds(networkRecoveryInterval)
            };
        }

        private IConnection GetConnection()
        {
            if (this.m_connection != null) return this.m_connection;
            lock (lockCreate)
            {
                if (m_connection == null)
                {
                    m_connection = this.m_connectionFactory.CreateConnection(this.clientName);
                    m_connection.CallbackException += conCallbackException;
                }
            }
            return this.m_connection;
        }

        private void conCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            if (CallbackException != null)
                CallbackException(e.Exception, e.Detail, string.Empty);
        }

        private IModel GetSubscribeChannel()
        {
            if (m_subChannel != null) return m_subChannel;
            var con = GetConnection();
            lock (lockCreate)
            {
                if (m_subChannel == null)
                {
                    m_subChannel = con.CreateModel();
                }
            }

            return m_subChannel;
        }

        private PublishChannel GetPublishChannel()
        {
            IModel ch = null;
            while (this.m_publishChannelQueue.TryDequeue(out ch) && !ch.IsOpen)
            {
                ch.Dispose();
                ch = null;
            }
            if (ch == null)
            {
                var con = GetConnection();
                ch = con.CreateModel();
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
        public virtual void ExchangeDeclare(string exchange = "amq.direct", string type = "direct", bool durable = true, bool autoDelete = false, IDictionary<string, object> arguments = null)
        {
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            if (string.IsNullOrEmpty(type)) throw new ArgumentNullException(nameof(type));
            using (var ph = GetPublishChannel())
            {
                ph.Channel.ExchangeDeclare(exchange, type, durable, autoDelete, arguments);
            }
        }

        /// <summary>
        /// ExchangeDeclare
        /// </summary>
        /// <param name="config"></param>
        public virtual void ExchangeDeclare(ExchangeConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            this.ExchangeDeclare(config.Exchange, config.Type, config.Durable, config.AutoDelete, config.Arguments);
        }

        /// <summary>
        /// 批量ExchangeDeclare
        /// </summary>
        /// <param name="configs"></param>
        public virtual void ExchangeDeclare(IEnumerable<ExchangeConfig> configs)
        {
            if (configs == null) throw new ArgumentNullException(nameof(configs));
            foreach (var item in configs)
            {
                if (item == null) throw new ArgumentNullException($"{nameof(configs)} item is null!");
                if (string.IsNullOrEmpty(item.Exchange)) throw new ArgumentNullException($"{nameof(configs)} item.{nameof(item.Exchange)} is null!");
                if (string.IsNullOrEmpty(item.Type)) throw new ArgumentNullException($"{nameof(configs)} item.{nameof(item.Type)} is null!");
            }
            using (var ph = GetPublishChannel())
            {
                foreach (var item in configs)
                {
                    ph.Channel.ExchangeDeclare(item.Exchange, item.Type, item.Durable, item.AutoDelete, item.Arguments);
                }
            }
        }

        /// <summary>
        /// QueueDeclare
        /// </summary>
        /// <param name="config"></param>
        public virtual void QueueDeclare(QueueConfig config)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (string.IsNullOrEmpty(config.Queue)) throw new ArgumentNullException(nameof(config.Queue));
            if (string.IsNullOrEmpty(config.Exchange)) throw new ArgumentNullException(nameof(config.Exchange));
            using (var ph = GetPublishChannel())
            {
                 ph.Channel.QueueDeclare(config.Queue, config.Durable, config.Exclusive, config.AutoDelete, config.QueueArguments);
                ph.Channel.QueueBind(config.Queue, config.Exchange, config.RoutingKey ?? string.Empty, config.BindArguments);
                if(!string.IsNullOrEmpty(config.DelayQueue) && config.Queue != config.DelayQueue 
                    && (config.RoutingKey != config.DelayRoutingKey || (string.IsNullOrEmpty(config.DelayRoutingKey) && string.IsNullOrEmpty(config.RoutingKey))))
                {
                    Dictionary<string, object> dic = new Dictionary<string, object>(2);
                    dic.Add("x-dead-letter-exchange", config.Exchange);
                    dic.Add("x-dead-letter-routing-key", config.RoutingKey ?? string.Empty);
                    ph.Channel.QueueDeclare(config.DelayQueue, config.Durable, config.Exclusive, config.AutoDelete, dic);
                    ph.Channel.QueueBind(config.DelayQueue, config.Exchange, config.DelayRoutingKey ?? string.Empty, null);
                }
            }
        }

        /// <summary>
        /// 批量QueueDeclare
        /// </summary>
        /// <param name="queues"></param>
        public virtual void QueueDeclare(IEnumerable<QueueConfig> queues)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            foreach (var item in queues)
            {
                if (item == null) throw new ArgumentNullException($"{nameof(queues)} item is null!");
                if (string.IsNullOrEmpty(item.Queue)) throw new ArgumentNullException($"{nameof(queues)} item.{nameof(item.Queue)} is null!");
                if (string.IsNullOrEmpty(item.Exchange)) throw new ArgumentNullException($"{nameof(queues)} item.{nameof(item.Exchange)} is null!");
            }
            using (var ph = GetPublishChannel())
            {
                foreach (var config in queues)
                {
                    var ok = ph.Channel.QueueDeclare(config.Queue, config.Durable, config.Exclusive, config.AutoDelete, config.QueueArguments);
                    ph.Channel.QueueBind(config.Queue, config.Exchange, config.RoutingKey ?? string.Empty, config.BindArguments);
                    if (!string.IsNullOrEmpty(config.DelayQueue) && config.Queue != config.DelayQueue
                    && (config.RoutingKey != config.DelayRoutingKey || (string.IsNullOrEmpty(config.DelayRoutingKey) && string.IsNullOrEmpty(config.RoutingKey))))
                    {
                        Dictionary<string, object> dic = new Dictionary<string, object>(2);
                        dic.Add("x-dead-letter-exchange", config.Exchange);
                        dic.Add("x-dead-letter-routing-key", config.RoutingKey ?? string.Empty);
                        ok = ph.Channel.QueueDeclare(config.DelayQueue, config.Durable, config.Exclusive, config.AutoDelete, dic);
                        ph.Channel.QueueBind(config.DelayQueue, config.Exchange, config.DelayRoutingKey ?? string.Empty, null);
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
        /// <returns>是否发生成功</returns>
        public virtual bool Publish<T>(T msg, string routingKey, TimeSpan? expire = null, string exchange = "amq.direct", bool persistent = false,
            IDictionary<string, object> headers = null)
        {
            if (msg == null) throw new ArgumentNullException(nameof(msg));
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            if (expire.HasValue && expire.Value.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(expire)}({expire}) is error!");
            var body = Serialize<T>(msg, out var contentType);
            using (var ph = GetPublishChannel())
            {
                IBasicProperties props = ph.Channel.CreateBasicProperties();
                props.Persistent = persistent;
                props.ContentType = contentType;
                props.ContentEncoding = "utf-8";
                if (expire.HasValue) props.Expiration = expire.Value.TotalMilliseconds.ToString("f0");
                if(headers != null) foreach(KeyValuePair<string, object> kv in headers) props.Headers[kv.Key] = kv.Value;
                ph.Channel.BasicPublish(exchange, routingKey ?? string.Empty, props, body);
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
        /// <returns></returns>
        public virtual bool Publish<T>(T msg, PubConfig config, TimeSpan? expire = null, bool persistent = false, 
            IDictionary<string, object> headers = null)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            return this.Publish(msg, config.RoutingKey, expire, config.Exchange, persistent, headers);
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
        /// <returns>是否发生成功</returns>
        public virtual bool Publish<T>(List<T> msgs, string routingKey, TimeSpan? expire = null, string exchange = "amq.direct", bool persistent = false, 
            IDictionary<string, object> headers = null)
        {
            if (msgs == null) throw new ArgumentNullException(nameof(msgs));
            if (msgs.Count == 0) return true;
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            if (expire.HasValue && expire.Value.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(expire)}({expire}) is error!");
            using (var ph = GetPublishChannel())
            {
                var ps = ph.Channel.CreateBasicPublishBatch();
                foreach (var m in msgs)
                {
                    var body = this.Serialize<T>(m, out var contentType);
                    IBasicProperties props = ph.Channel.CreateBasicProperties();
                    props.Persistent = persistent;
                    props.ContentType = contentType;
                    props.ContentEncoding = "utf-8";
                    if (expire.HasValue) props.Expiration = expire.Value.TotalMilliseconds.ToString("f0");
                    if (headers != null) foreach (KeyValuePair<string, object> kv in headers) props.Headers[kv.Key] = kv.Value;
                    ps.Add(exchange, routingKey ?? string.Empty, true, props, body);
                }
                ps.Publish();
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
        /// <returns></returns>
        public virtual bool Publish<T>(List<T> msgs, PubConfig config, TimeSpan? expire = null, bool persistent = false, 
            IDictionary<string, object> headers = null)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            return this.Publish(msgs, config.RoutingKey, expire, config.Exchange, persistent, headers);
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
        /// <returns>是否发生成功</returns>
        public virtual bool PublishDelay<T>(T msg, string delayRoutingKey, TimeSpan delay, string exchange = "amq.direct", bool persistent = false, 
            IDictionary<string, object> headers = null)
        {
            if (delay.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(delay)} is error!");

            return this.Publish(msg, delayRoutingKey, delay, exchange, persistent, headers);
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
        /// <returns>是否发生成功</returns>
        public virtual bool PublishDelay<T>(T msg, PubConfig config, TimeSpan delay, bool persistent = false, 
            IDictionary<string, object> headers = null)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));

            return this.Publish(msg, config.DelayRoutingKey, delay, config.Exchange, persistent, headers);
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
        /// <returns>是否发生成功</returns>
        public virtual bool PublishDelay<T>(List<T> msgs, string delayRoutingKey, TimeSpan delay, string exchange = "amq.direct", bool persistent = false, 
            IDictionary<string, object> headers = null)
        {
            if (msgs == null) throw new ArgumentNullException(nameof(msgs));
            if (msgs.Count == 0) return true;
            if (delay.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(delay)} is error!");

            return this.Publish(msgs, delayRoutingKey, delay, exchange, persistent, headers);
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
        /// <returns>是否发生成功</returns>
        public virtual bool PublishDelay<T>(List<T> msgs, PubConfig config, TimeSpan delay, bool persistent = false, 
            IDictionary<string, object> headers = null)
        {
            if (msgs == null) throw new ArgumentNullException(nameof(msgs));
            if (msgs.Count == 0) return true;
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (delay.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(delay)} is error!");

            return this.Publish(msgs, config.DelayRoutingKey, delay, config.Exchange, persistent, headers);
        }

        #endregion

        private T Deserialize<T>(ReadOnlyMemory<byte> buffer)
        {
            T result = default(T);
            var t = typeof(T);
            if (buffer is  T)
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
        public virtual void Subscribe<T>(SubscribeHander<T> hander, string queue, bool autoAck = false)
        {
            if (this.m_connectionFactory.DispatchConsumersAsync) throw new InvalidOperationException("ConsumersAsync is true, no support！");
            if (hander == null) throw new ArgumentNullException(nameof(hander));
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            var channel = GetSubscribeChannel();
            lock (lockSubChannel)
            {
                channel.BasicQos(0, 1, false);
                var eventingBasicConsumer = new EventingBasicConsumer(channel);
                var consumer = new Consumer<T>(this, eventingBasicConsumer, hander, autoAck);
                this.consumerList.Add(consumer);
                eventingBasicConsumer.Received += consumer.Handler;
                channel.BasicConsume(queue, autoAck, eventingBasicConsumer);
            }
        }

        /// <summary>
        /// 消费消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="hander"></param>
        /// <param name="queue"></param>
        /// <param name="autoAck">是否自动确认</param>
        /// <param name="newTask">是否使用新线程</param>
        public virtual void Subscribe<T>(AsyncSubscribeHander<T> hander, string queue, bool autoAck = false, bool newTask = false)
        {
            if(!this.m_connectionFactory.DispatchConsumersAsync) throw new InvalidOperationException("ConsumersAsync is false, no support！");
            if (hander == null) throw new ArgumentNullException(nameof(hander));
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            var channel = GetSubscribeChannel();
            lock (lockSubChannel)
            {
                channel.BasicQos(0, 1, false);
                var eventingBasicConsumer = new AsyncEventingBasicConsumer(channel);
                var consumer = new AsyncConsumer<T>(this, eventingBasicConsumer, hander, autoAck, newTask);
                this.consumerList.Add(consumer);
                eventingBasicConsumer.Received += consumer.Handler;
                channel.BasicConsume(queue, autoAck, eventingBasicConsumer);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
        }

        private object disObj = new object();
        /// <summary>
        /// 
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                lock (this.disObj)
                {
                    if (this.m_subChannel != null)
                    {
                        try { if (this.m_subChannel.IsOpen) this.m_subChannel.Close(); } catch { }
                        try { this.m_subChannel.Dispose(); } catch { }
                    }
                    this.m_subChannel = null;
                    IModel model;
                    while (this.m_publishChannelQueue != null && this.m_publishChannelQueue.TryDequeue(out model))
                    {
                        if (model != null)
                        {
                            try { if (model.IsOpen) model.Close(); } catch { }
                            try { model.Dispose(); } catch { }
                        }
                    }
                    this.m_publishChannelQueue = null;
                    if (this.m_connection != null)
                    {
                        try { if (this.m_connection.IsOpen) this.m_connection.Close(); } catch { }
                        try { this.m_connection.Dispose(); } catch { }
                    }
                    this.m_connection = null;
                    this.CallbackException = null;
                    this.lockSubChannel = null;

                    if(this.consumerList != null)
                    {
                        foreach (var c in this.consumerList) c.Dispose();
                        this.consumerList.Clear();
                        this.consumerList.TrimExcess();
                    }
                    this.consumerList = null;
                }
            }
        }

        class PublishChannel : IDisposable
        {
            private MQPool pool;
            public IModel Channel { get; private set; }
            public PublishChannel(MQPool pool, IModel channel)
            {
                if (pool == null) throw new ArgumentNullException(nameof(pool));
                if (channel == null) throw new ArgumentNullException(nameof(channel));
                this.pool = pool;
                this.Channel = channel;
            }

            public void Dispose()
            {
                if (this.pool != null)
                {
                    if (this.pool.maxPool > this.pool.m_publishChannelQueue.Count && this.Channel.IsOpen)
                    {
                        this.pool.m_publishChannelQueue.Enqueue(this.Channel);
                    }
                    else
                    {
                        if (this.Channel.IsOpen) this.Channel.Close();
                        this.Channel.Dispose();
                    }
                }
                this.pool = null;
                this.Channel = null;
            }
        }

        class ConsumerBase: IDisposable
        {
            protected MQPool pool;

            public ConsumerBase(MQPool pool)
            {
                this.pool = pool;
            }

            public virtual void Dispose(bool disposable)
            {
                if (disposable)
                {
                    this.pool = null;
                }
            }

            public void Dispose()
            {
                this.Dispose(true);
            }
        }

        class Consumer<T> : ConsumerBase
        {
            private EventingBasicConsumer consumer;
            private SubscribeHander<T> hander;
            private bool autoAck;

            public Consumer(MQPool pool, EventingBasicConsumer consumer, SubscribeHander<T> hander, bool autoAck)
                : base(pool)
            {
                this.consumer = consumer;
                this.hander = hander;
                this.autoAck = autoAck;
            }

            public void Handler(object sender, BasicDeliverEventArgs e)
            {
                bool handerOk = false;
                try
                {
                    T m = this.pool.Deserialize<T>(e.Body);
                    if (m != null) handerOk = hander(m, e.BasicProperties?.Headers);
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
                        consumer.Model.BasicAck(e.DeliveryTag, false);
                    }
                    else
                    {
                        consumer.Model.BasicNack(e.DeliveryTag, false, true);
                    }
                }
            }

           public override void Dispose(bool disposable)
            {
                if (disposable)
                {
                    this.consumer = null;
                    this.hander = null;
                }
                base.Dispose(disposable);
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
                        consumer.Model.BasicAck(e.DeliveryTag, false);
                    }
                    else
                    {
                        consumer.Model.BasicNack(e.DeliveryTag, false, true);
                    }
                }
            }

            public override void Dispose(bool disposable)
            {
                if (disposable)
                {
                    this.consumer = null;
                    this.hander = null;
                }
                base.Dispose(disposable);
            }
        }
    }
}