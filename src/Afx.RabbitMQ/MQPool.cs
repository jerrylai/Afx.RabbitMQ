using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Afx.RabbitMQ.Json;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
#if NETCOREAPP || NETSTANDARD
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
#else
using Newtonsoft.Json;
#endif

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

        private IModel m_subChannel;
        private object lockSubChannel = new object();
        private List<ConsumerBase> consumerList = new List<ConsumerBase>();

        private readonly int maxPool = 5;
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

#if NETCOREAPP || NETSTANDARD
        private static readonly JsonSerializerOptions jsonOptions = new JsonSerializerOptions()
        {
            IgnoreNullValues = true,
            WriteIndented = true,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            PropertyNameCaseInsensitive = false,
            PropertyNamingPolicy = null,
            DictionaryKeyPolicy = null
        };
        private JsonSerializerOptions options = jsonOptions;

        static MQPool()
        {
            jsonOptions.Converters.Add(new StringJsonConverter());
            jsonOptions.Converters.Add(new BooleanJsonConverter());
            jsonOptions.Converters.Add(new IntJsonConverter());
            jsonOptions.Converters.Add(new LongJsonConverter());
            jsonOptions.Converters.Add(new FloatJsonConverter());
            jsonOptions.Converters.Add(new DoubleJsonConverter());
            jsonOptions.Converters.Add(new DecimalJsonConverter());
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="jsonSerializerOptions"></param>
        public void SetJsonOptions(JsonSerializerOptions jsonSerializerOptions)
        {
            if (jsonSerializerOptions != null) this.options = jsonSerializerOptions;
        }
#else
        private static readonly JsonSerializerSettings jsonOptions = new JsonSerializerSettings()
        {
            NullValueHandling = NullValueHandling.Ignore,
            MissingMemberHandling = Newtonsoft.Json.MissingMemberHandling.Ignore
        };
        private JsonSerializerSettings options = jsonOptions;
        /// <summary>
        /// 
        /// </summary>
        /// <param name="jsonSerializerOptions"></param>
        public void SetJsonOptions(JsonSerializerSettings jsonSerializerOptions)
        {
            if (jsonSerializerOptions != null) this.options = jsonSerializerOptions;
        }
#endif

        /// <summary>
        /// mq应用池
        /// </summary>
        /// <param name="hostName">mq服务器</param>
        /// <param name="port">mq端口</param>
        /// <param name="userName">登录账号</param>
        /// <param name="password">密码</param>
        /// <param name="virtualHost"></param>
        /// <param name="maxPool">push池大小</param>
        /// <param name="networkRecoveryInterval"></param>
        /// <param name="clientName"></param>
        /// <param name="consumersAsync">async 消费</param>
        public MQPool(string hostName, int port, string userName, string password, string virtualHost, int maxPool = 5, int networkRecoveryInterval = 15, string clientName = null, bool consumersAsync = true)
        {
            if (string.IsNullOrEmpty(hostName)) throw new ArgumentNullException(nameof(virtualHost));
            if (port <= System.Net.IPEndPoint.MinPort || System.Net.IPEndPoint.MaxPort <= port) throw new ArgumentException(nameof(port));
            if (string.IsNullOrEmpty(userName)) throw new ArgumentNullException(nameof(userName));
            if (virtualHost == null) throw new ArgumentNullException(nameof(virtualHost));
            if (maxPool < 0) throw new ArgumentException(nameof(maxPool));
            if (networkRecoveryInterval <= 0) throw new ArgumentException(nameof(networkRecoveryInterval));
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
                var ok = ph.Channel.QueueDeclare(config.Queue, config.Durable, config.Exclusive, config.AutoDelete, config.QueueArguments);
                ph.Channel.QueueBind(config.Queue, config.Exchange, config.RoutingKey, config.BindArguments);
                if(!string.IsNullOrEmpty(config.DelayQueue) && !string.IsNullOrEmpty(config.DelayRoutingKey) && !string.IsNullOrEmpty(config.RoutingKey)
                    && config.Queue != config.DelayQueue && config.RoutingKey != config.DelayRoutingKey)
                {
                    Dictionary<string, object> dic = new Dictionary<string, object>(2);
                    dic.Add("x-dead-letter-exchange", config.Exchange);
                    dic.Add("x-dead-letter-routing-key", config.RoutingKey);
                    ok = ph.Channel.QueueDeclare(config.DelayQueue, config.Durable, config.Exclusive, config.AutoDelete, dic);
                    ph.Channel.QueueBind(config.DelayQueue, config.Exchange, config.DelayRoutingKey, null);
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
                foreach (var item in queues)
                {
                    var ok = ph.Channel.QueueDeclare(item.Queue, item.Durable, item.Exclusive, item.AutoDelete, item.QueueArguments);
                    ph.Channel.QueueBind(item.Queue, item.Exchange, item.RoutingKey, item.BindArguments);
                    if (!string.IsNullOrEmpty(item.DelayQueue) && !string.IsNullOrEmpty(item.DelayRoutingKey) && !string.IsNullOrEmpty(item.RoutingKey)
                        && item.Queue != item.DelayQueue && item.RoutingKey != item.DelayRoutingKey)
                    {
                        Dictionary<string, object> dic = new Dictionary<string, object>(2);
                        dic.Add("x-dead-letter-exchange", item.Exchange);
                        dic.Add("x-dead-letter-routing-key", item.RoutingKey);
                        ok = ph.Channel.QueueDeclare(item.DelayQueue, item.Durable, item.Exclusive, item.AutoDelete, dic);
                        ph.Channel.QueueBind(item.DelayQueue, item.Exchange, item.DelayRoutingKey, null);
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
#if NETCOREAPP || NETSTANDARD
                var json = JsonSerializer.Serialize(m, this.options);
#else
                var json = JsonConvert.SerializeObject(m, this.options);
#endif
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
        /// <param name="serialize">自定义序列化</param>
        /// <returns>是否发生成功</returns>
        public virtual bool Publish<T>(T msg, string routingKey, TimeSpan? expire = null,
            string exchange = "amq.direct", bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null)
        {
            if (msg == null) throw new ArgumentNullException(nameof(msg));
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            if (expire.HasValue && expire.Value.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(expire)}({expire}) is error!");
            bool result = true;
            string contentType = "application/octet-stream";
            var body = serialize != null ? serialize(msg) : Serialize<T>(msg, out contentType);
            using (var ph = GetPublishChannel())
            {
                //ph.Channel.ConfirmSelect();
                IBasicProperties props = ph.Channel.CreateBasicProperties();
                props.Persistent = persistent;
                props.ContentType = contentType;
                props.ContentEncoding = "utf-8";
                if (expire.HasValue) props.Expiration = expire.Value.TotalMilliseconds.ToString("f0");
                ph.Channel.BasicPublish(exchange, routingKey ?? string.Empty, props, body);
                //result = ph.Channel.WaitForConfirms();
            }

            return result;
        }

        /// <summary>
        /// 发布消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="expire">消息过期时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="serialize">自定义序列化</param>
        /// <returns></returns>
        public virtual bool Publish<T>(T msg, PubMsgConfig config, TimeSpan? expire = null, bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            return this.Publish(msg, config.RoutingKey, expire, config.Exchange, persistent, serialize);
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
        /// <param name="serialize">自定义序列化</param>
        /// <returns>是否发生成功</returns>
        public virtual bool Publish<T>(List<T> msgs, string routingKey, TimeSpan? expire = null,
            string exchange = "amq.direct", bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null)
        {
            if (msgs == null) throw new ArgumentNullException(nameof(msgs));
            if (msgs.Count == 0) return true;
            if (string.IsNullOrEmpty(routingKey)) throw new ArgumentNullException(nameof(routingKey));
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            if (expire.HasValue && expire.Value.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(expire)}({expire}) is error!");
            bool result = true;
            string contentType = "application/octet-stream";
            using (var ph = GetPublishChannel())
            {
                //ph.Channel.ConfirmSelect();
                var ps = ph.Channel.CreateBasicPublishBatch();
                foreach (var m in msgs)
                {
                    var body = serialize != null ? serialize(m) : Serialize<T>(m, out contentType);
                    IBasicProperties props = ph.Channel.CreateBasicProperties();
                    props.Persistent = persistent;
                    props.ContentType = contentType;
                    props.ContentEncoding = "utf-8";
                    if (expire.HasValue) props.Expiration = expire.Value.TotalMilliseconds.ToString("f0");

                    ps.Add(exchange, routingKey ?? string.Empty, true, props, body);
                }
                ps.Publish();
                // result = ph.Channel.WaitForConfirms();
            }
            return result;
        }

        /// <summary>
        /// 发布消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msgs">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="expire">消息过期时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="serialize">自定义序列化</param>
        /// <returns></returns>
        public virtual bool Publish<T>(List<T> msgs, PubMsgConfig config, TimeSpan? expire = null, bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            return this.Publish(msgs, config.RoutingKey, expire, config.Exchange, persistent, serialize);
        }

        /// <summary>
        /// 发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg">消息</param>
        /// <param name="delayRoutingKey">routingKey</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="exchange">exchange</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="serialize">自定义序列化</param>
        /// <returns>是否发生成功</returns>
        public virtual bool PublishDelay<T>(T msg, string delayRoutingKey, TimeSpan delay,
            string exchange = "amq.direct", bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null)
        {
            if (string.IsNullOrEmpty(delayRoutingKey)) throw new ArgumentNullException(nameof(delayRoutingKey));
            if (delay.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(delay)} is error!");

            return this.Publish(msg, delayRoutingKey, delay, exchange, persistent, serialize);
        }

        /// <summary>
        /// 发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="serialize">自定义序列化</param>
        /// <returns>是否发生成功</returns>
        public virtual bool PublishDelay<T>(T msg, PubMsgConfig config, TimeSpan delay, bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (string.IsNullOrEmpty(config.DelayRoutingKey)) throw new ArgumentNullException(nameof(config.DelayRoutingKey));

            return this.Publish(msg, config.DelayRoutingKey, delay, config.Exchange, persistent, serialize);
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
        /// <param name="serialize">自定义序列化</param>
        /// <returns>是否发生成功</returns>
        public virtual bool PublishDelay<T>(List<T> msgs, string delayRoutingKey, TimeSpan delay,
            string exchange = "amq.direct", bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null)
        {
            if (msgs == null) throw new ArgumentNullException(nameof(msgs));
            if (msgs.Count == 0) return true;
            if (string.IsNullOrEmpty(delayRoutingKey)) throw new ArgumentNullException(nameof(delayRoutingKey));
            if (delay.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(delay)} is error!");

            return this.Publish(msgs, delayRoutingKey, delay, exchange, persistent, serialize);
        }

        /// <summary>
        /// 发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msgs">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="serialize">自定义序列化</param>
        /// <returns>是否发生成功</returns>
        public virtual bool PublishDelay<T>(List<T> msgs, PubMsgConfig config, TimeSpan delay, bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null)
        {
            if (msgs == null) throw new ArgumentNullException(nameof(msgs));
            if (msgs.Count == 0) return true;
            if (config == null) throw new ArgumentNullException(nameof(config));
            if (string.IsNullOrEmpty(config.DelayRoutingKey)) throw new ArgumentNullException(nameof(config.DelayRoutingKey));
            if (delay.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(delay)} is error!");

            return this.Publish(msgs, config.DelayRoutingKey, delay, config.Exchange, persistent, serialize);
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
#if NETCOREAPP || NETSTANDARD
                    result = JsonSerializer.Deserialize<T>(json, this.options);
#else
                    result =  JsonConvert.DeserializeObject<T>(json, this.options);
#endif
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
        /// <param name="deserialize">自定义反序列化</param>
        public virtual void Subscribe<T>(SubscribeHander<T> hander, string queue, bool autoAck = false, Func<ReadOnlyMemory<byte>, T> deserialize = null)
        {
            if (this.m_connectionFactory.DispatchConsumersAsync) throw new InvalidOperationException("ConsumersAsync is true, no support！");
            if (hander == null) throw new ArgumentNullException(nameof(hander));
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            var channel = GetSubscribeChannel();
            lock (lockSubChannel)
            {
                channel.BasicQos(0, 1, false);
                var eventingBasicConsumer = new EventingBasicConsumer(channel);
                var consumer = new Consumer<T>(this, eventingBasicConsumer, hander, autoAck, deserialize);
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
        /// <param name="deserialize">自定义反序列化</param>
        public virtual void Subscribe<T>(AsyncSubscribeHander<T> hander, string queue, bool autoAck = false, Func<ReadOnlyMemory<byte>, T> deserialize = null)
        {
            if(!this.m_connectionFactory.DispatchConsumersAsync) throw new InvalidOperationException("ConsumersAsync is false, no support！");
            if (hander == null) throw new ArgumentNullException(nameof(hander));
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            var channel = GetSubscribeChannel();
            lock (lockSubChannel)
            {
                channel.BasicQos(0, 1, false);
                var eventingBasicConsumer = new AsyncEventingBasicConsumer(channel);
                var consumer = new AsyncConsumer<T>(this, eventingBasicConsumer, hander, autoAck, deserialize);
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
            private Func<ReadOnlyMemory<byte>, T> deserialize;

            public Consumer(MQPool pool, EventingBasicConsumer consumer, SubscribeHander<T> hander, bool autoAck, Func<ReadOnlyMemory<byte>, T> deserialize)
                : base(pool)
            {
                this.consumer = consumer;
                this.hander = hander;
                this.autoAck = autoAck;
                this.deserialize = deserialize;
            }

            private T Deserialize(ReadOnlyMemory<byte> read)
            {
                if (this.deserialize != null) return deserialize(read);
                else return this.pool.Deserialize<T>(read);
            }

            public void Handler(object sender, BasicDeliverEventArgs e)
            {
                bool handerOk = false;
                try
                {
                    T m = this.Deserialize(e.Body);
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
                    this.deserialize = null;
                }
                base.Dispose(disposable);
            }
        }

        class AsyncConsumer<T> : ConsumerBase
        {
            private AsyncEventingBasicConsumer consumer;
            private AsyncSubscribeHander<T> hander;
            private bool autoAck;
            private Func<ReadOnlyMemory<byte>, T> deserialize;

            public AsyncConsumer(MQPool pool, AsyncEventingBasicConsumer consumer, AsyncSubscribeHander<T> hander, bool autoAck, Func<ReadOnlyMemory<byte>, T> deserialize)
                : base(pool)
            {
                this.consumer = consumer;
                this.hander = hander;
                this.autoAck = autoAck;
                this.deserialize = deserialize;
            }

            private T Deserialize(ReadOnlyMemory<byte> read)
            {
                if (this.deserialize != null) return deserialize(read);
                else return this.pool.Deserialize<T>(read);
            }

            public async Task Handler(object sender, BasicDeliverEventArgs e)
            {
                bool handerOk = false;
                try
                {
                    T m = this.Deserialize(e.Body);
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
                    this.deserialize = null;
                }
                base.Dispose(disposable);
            }
        }
    }
}