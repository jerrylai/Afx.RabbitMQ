using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
#if NETCOREAPP || NETSTANDARD
using System.Text.Json;
using System.Text.Json.Serialization;
#else
using Newtonsoft.Json;
#endif
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Afx.RabbitMQ
{
    /// <summary>
    /// mq 应用池
    /// </summary>
    public class MQPool : IMQPool
    {
#if NETCOREAPP || NETSTANDARD
        private static readonly JsonSerializerOptions jsonOptions;
        static MQPool()
        {
            jsonOptions = new JsonSerializerOptions()
            {
                IgnoreNullValues = true,
                WriteIndented = false,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
                PropertyNameCaseInsensitive = false,
                PropertyNamingPolicy = null,
                DictionaryKeyPolicy = null
            };
            jsonOptions.Converters.Add(new StringJsonConverter());
            jsonOptions.Converters.Add(new BooleanJsonConverter());
            jsonOptions.Converters.Add(new IntJsonConverter());
            jsonOptions.Converters.Add(new LongJsonConverter());
            jsonOptions.Converters.Add(new FloatJsonConverter());
            jsonOptions.Converters.Add(new DoubleJsonConverter());
            jsonOptions.Converters.Add(new DecimalJsonConverter());
        }
#else
        private static readonly JsonSerializerSettings jsonOptions = new JsonSerializerSettings()
        {
            NullValueHandling = NullValueHandling.Ignore,
            MissingMemberHandling = Newtonsoft.Json.MissingMemberHandling.Ignore
        };
#endif
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

        private object lockCreate = new object();
        private IConnectionFactory m_connectionFactory;
        private IConnection m_connection;
        private string clientName { get; set; }

        private IModel m_subChannel;
        private object lockSubChannel = new object();

        private readonly int maxPool = 5;
        private ConcurrentQueue<IModel> m_publishChannelQueue = new ConcurrentQueue<IModel>();

        private const string DELAY_QUEUE = "delay";
        private object delayQueueObj = new object();
        private ConcurrentDictionary<string, string> delayQueueDic = new ConcurrentDictionary<string, string>();

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
        /// <param name="virtualHost"></param>
        /// <param name="maxPool">push池大小</param>
        /// <param name="networkRecoveryInterval"></param>
        /// <param name="clientName"></param>
        public MQPool(string hostName, int port, string userName, string password, string virtualHost, int maxPool = 5, int networkRecoveryInterval = 15, string clientName = null)
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

        #region Declare
        /// <summary>
        /// ExchangeDeclare
        /// </summary>
        /// <param name="exchange">exchange</param>
        /// <param name="durable">是否持久化</param>
        /// <param name="autoDelete">当已经没有消费者时，服务器是否可以删除该Exchange</param>
        /// <param name="type">direct、fanout、topic</param>
        /// <param name="arguments"></param>
        public virtual void ExchangeDeclare(string exchange = "amq.topic", bool durable = true, bool autoDelete = false,
            string type = "topic", IDictionary<string, object> arguments = null)
        {
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            if (string.IsNullOrEmpty(type)) throw new ArgumentNullException(nameof(type));
            using (var ph = GetPublishChannel())
            {
                ph.Channel.ExchangeDeclare(exchange, type, durable, autoDelete, arguments);
            }
        }

        /// <summary>
        /// QueueDeclare
        /// </summary>
        /// <param name="queue">queue</param>
        /// <param name="routingKey">routingKey</param>
        /// <param name="durable">是否持久化</param>
        /// <param name="exclusive">连接断开是否删除队列</param>
        /// <param name="autoDelete">当已经没有消费者时，服务器是否可以删除该Exchange</param>
        /// <param name="exchange">exchange</param>
        /// <param name="queueArguments">queueArguments</param>
        /// <param name="bindArguments">bindArguments</param>
        public virtual void QueueDeclare(string queue, string routingKey, bool durable = true, bool exclusive = false, bool autoDelete = false,
             string exchange = "amq.topic", IDictionary<string, object> queueArguments = null, IDictionary<string, object> bindArguments = null)
        {
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            if (string.IsNullOrEmpty(routingKey)) throw new ArgumentNullException(nameof(routingKey));
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            using (var ph = GetPublishChannel())
            {
                var ok = ph.Channel.QueueDeclare(queue, durable, exclusive, autoDelete, queueArguments);
                ph.Channel.QueueBind(queue, exchange, routingKey, bindArguments);
            }
        }

        /// <summary>
        /// 批量ExchangeDeclare
        /// </summary>
        /// <param name="exchanges"></param>
        public virtual void ExchangeDeclare(IEnumerable<ExchangeConfig> exchanges)
        {
            if (exchanges == null) throw new ArgumentNullException(nameof(exchanges));
            foreach (var item in exchanges)
            {
                if (item == null) throw new ArgumentNullException($"{nameof(exchanges)} item is null!");
                if (string.IsNullOrEmpty(item.Exchange)) throw new ArgumentNullException($"{nameof(exchanges)} item.{nameof(item.Exchange)} is null!");
                if (string.IsNullOrEmpty(item.Type)) throw new ArgumentNullException($"{nameof(exchanges)} item.{nameof(item.Type)} is null!");
            }
            using (var ph = GetPublishChannel())
            {
                foreach (var item in exchanges)
                {
                    ph.Channel.ExchangeDeclare(item.Exchange, item.Type, item.Durable, item.AutoDelete, item.Arguments);
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
                if (string.IsNullOrEmpty(item.RoutingKey)) throw new ArgumentNullException($"{nameof(queues)} item.{nameof(item.RoutingKey)} is null!");
                if (string.IsNullOrEmpty(item.Exchange)) throw new ArgumentNullException($"{nameof(queues)} item.{nameof(item.Exchange)} is null!");
            }
            using (var ph = GetPublishChannel())
            {
                foreach (var item in queues)
                {
                    var ok = ph.Channel.QueueDeclare(item.Queue, item.Durable, item.Exclusive, item.AutoDelete, item.QueueArguments);
                    ph.Channel.QueueBind(item.Queue, item.Exchange, item.RoutingKey, item.BindArguments);
                }
            }
        }

        #endregion

        #region
        private byte[] Serialize<T>(T m, out string contentType)
        {
            contentType = null;
            var t = typeof(T);
            byte[] result = null;
            if (t == typeof(byte[]))
            {
                contentType = "application/octet-stream";
                result = m as byte[];
            }
            else if (t == typeof(string))
            {
                contentType = "text/plain";
                result = Encoding.UTF8.GetBytes(m as string);
            }
            else
            {
#if NETCOREAPP || NETSTANDARD
                var json = JsonSerializer.Serialize(m, jsonOptions);
#else
                    var json = Newtonsoft.Json.JsonConvert.SerializeObject(m, jsonOptions);
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
        /// <returns>是否发生成功</returns>
        public virtual bool Publish<T>(T msg, string routingKey, TimeSpan? expire = null,
            string exchange = "amq.topic", bool persistent = false)
        {
            if (msg == null) throw new ArgumentNullException(nameof(msg));
            if (string.IsNullOrEmpty(routingKey)) throw new ArgumentNullException(nameof(routingKey));
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            if (expire.HasValue && expire.Value.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(expire)}({expire}) is error!");
            bool result = true;
            string contentType = null;
            var body = Serialize<T>(msg, out contentType);
            using (var ph = GetPublishChannel())
            {
                //ph.Channel.ConfirmSelect();
                IBasicProperties props = ph.Channel.CreateBasicProperties();
                props.Persistent = persistent;
                props.ContentType = contentType;
                props.ContentEncoding = "utf-8";
                if (expire.HasValue) props.Expiration = expire.Value.TotalMilliseconds.ToString("f0");
                ph.Channel.BasicPublish(exchange, routingKey, props, body);
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
        /// <returns></returns>
        public virtual bool Publish<T>(T msg, PubMsgConfig config, TimeSpan? expire = null, bool persistent = false)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            return this.Publish(msg, config.RoutingKey, expire, config.Exchange, persistent);
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
        /// <returns>是否发生成功</returns>
        public virtual bool Publish<T>(List<T> msgs, string routingKey, TimeSpan? expire = null,
            string exchange = "amq.topic", bool persistent = false)
        {
            if (msgs == null) throw new ArgumentNullException(nameof(msgs));
            if (msgs.Count == 0) return true;
            if (string.IsNullOrEmpty(routingKey)) throw new ArgumentNullException(nameof(routingKey));
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            if (expire.HasValue && expire.Value.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(expire)}({expire}) is error!");
            bool result = true;
            string contentType = null;
            using (var ph = GetPublishChannel())
            {
                //ph.Channel.ConfirmSelect();
                var ps = ph.Channel.CreateBasicPublishBatch();
                foreach (var m in msgs)
                {
                    var body = Serialize<T>(m, out contentType);
                    IBasicProperties props = ph.Channel.CreateBasicProperties();
                    props.Persistent = persistent;
                    props.ContentType = contentType;
                    props.ContentEncoding = "utf-8";
                    if (expire.HasValue) props.Expiration = expire.Value.TotalMilliseconds.ToString("f0");

                    ps.Add(exchange, routingKey, true, props, new ReadOnlyMemory<byte>(body));
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
        /// <returns></returns>
        public virtual bool Publish<T>(List<T> msgs, PubMsgConfig config, TimeSpan? expire = null, bool persistent = false)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            return this.Publish(msgs, config.RoutingKey, expire, config.Exchange, persistent);
        }

        /// <summary>
        /// 发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg">消息</param>
        /// <param name="routingKey">routingKey</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="exchange">exchange</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <returns>是否发生成功</returns>
        public virtual bool PublishDelay<T>(T msg, string routingKey, TimeSpan delay,
            string exchange = "amq.topic", bool persistent = false)
        {
            if (msg == null) throw new ArgumentNullException(nameof(msg));
            if (string.IsNullOrEmpty(routingKey)) throw new ArgumentNullException(nameof(routingKey));
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            if (delay.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(delay)} is error!");
            bool result = true;
            string contentType = null;
            var body = Serialize(msg, out contentType);
            using (var ph = GetPublishChannel())
            {
                string queue = null;
                var kv = $"{exchange}|{routingKey}";
                if (!delayQueueDic.TryGetValue(kv, out queue))
                {
                    lock (delayQueueObj)
                    {
                        if (!delayQueueDic.TryGetValue(kv, out queue))
                        {
                            queue = $"{DELAY_QUEUE}.{Guid.NewGuid().ToString("n")}";
                            Dictionary<string, object> dic = new Dictionary<string, object>(2);
                            dic.Add("x-dead-letter-exchange", exchange);
                            dic.Add("x-dead-letter-routing-key", routingKey);
                            ph.Channel.QueueDeclare(queue, true, true, false, dic);
                            ph.Channel.QueueBind(queue, exchange, queue, null);
                            delayQueueDic.TryAdd(kv, queue);
                        }
                    }
                }

                //ph.Channel.ConfirmSelect();
                IBasicProperties props = ph.Channel.CreateBasicProperties();
                props.Persistent = persistent;
                props.ContentType = contentType;
                props.ContentEncoding = "utf-8";
                props.Expiration = delay.TotalMilliseconds.ToString("f0");

                ph.Channel.BasicPublish(exchange, queue, props, body);
                //result = ph.Channel.WaitForConfirms();
            }
            return result;
        }

        /// <summary>
        /// 发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <returns>是否发生成功</returns>
        public virtual bool PublishDelay<T>(T msg, PubMsgConfig config, TimeSpan delay, bool persistent = false)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            return this.PublishDelay(msg, config.RoutingKey, delay, config.Exchange, persistent);
        }

        /// <summary>
        /// 批量发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msgs">消息</param>
        /// <param name="routingKey">routingKey</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="exchange">exchange</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <returns>是否发生成功</returns>
        public virtual bool PublishDelay<T>(List<T> msgs, string routingKey, TimeSpan delay,
            string exchange = "amq.topic", bool persistent = false)
        {
            if (msgs == null) throw new ArgumentNullException(nameof(msgs));
            if (msgs.Count == 0) return true;
            if (string.IsNullOrEmpty(routingKey)) throw new ArgumentNullException(nameof(routingKey));
            if (string.IsNullOrEmpty(exchange)) throw new ArgumentNullException(nameof(exchange));
            if (delay.TotalMilliseconds < 1) throw new ArgumentException($"{nameof(delay)} is error!");
            bool result = true;
            string contentType = null;
            using (var ph = GetPublishChannel())
            {
                string queue = null;
                var kv = $"{exchange}|{routingKey}";
                if (!delayQueueDic.TryGetValue(kv, out queue))
                {
                    lock (delayQueueObj)
                    {
                        if (!delayQueueDic.TryGetValue(kv, out queue))
                        {
                            queue = $"{DELAY_QUEUE}.{Guid.NewGuid().ToString("n")}";
                            Dictionary<string, object> dic = new Dictionary<string, object>(2);
                            dic.Add("x-dead-letter-exchange", exchange);
                            dic.Add("x-dead-letter-routing-key", routingKey);
                            ph.Channel.QueueDeclare(queue, true, true, false, dic);
                            ph.Channel.QueueBind(queue, exchange, queue, dic);
                            delayQueueDic.TryAdd(kv, queue);
                        }
                    }
                }

                //ph.Channel.ConfirmSelect();
                var ps = ph.Channel.CreateBasicPublishBatch();
                foreach (var m in msgs)
                {
                    byte[] body = Serialize<T>(m, out contentType);
                    IBasicProperties props = ph.Channel.CreateBasicProperties();
                    props.Persistent = persistent;
                    props.ContentType = contentType;
                    props.ContentEncoding = "utf-8";
                    props.Expiration = delay.TotalMilliseconds.ToString("f0");

                    ps.Add(exchange, queue, true, props, new ReadOnlyMemory<byte>(body));
                }
                ps.Publish();
                //result = ph.Channel.WaitForConfirms();
            }
            return result;
        }

        /// <summary>
        /// 发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msgs">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <returns>是否发生成功</returns>
        public virtual bool PublishDelay<T>(List<T> msgs, PubMsgConfig config, TimeSpan delay, bool persistent = false)
        {
            if (config == null) throw new ArgumentNullException(nameof(config));
            return this.PublishDelay(msgs, config.RoutingKey, delay, config.Exchange, persistent);
        }

        #endregion

        private T Deserialize<T>(ReadOnlyMemory<byte> buffer)
        {
            T result = default(T);
            var t = typeof(T);
            if (t == typeof(byte[]))
            {
                result = (T)((object)buffer.ToArray());
            }
            else if (t == typeof(string))
            {
                result = (T)((object)Encoding.UTF8.GetString(buffer.ToArray()));
            }
            else
            {
                var json = Encoding.UTF8.GetString(buffer.ToArray());
                try
                {
#if NETCOREAPP || NETSTANDARD
                    result = JsonSerializer.Deserialize<T>(json, jsonOptions);
#else
                    result = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(json, jsonOptions);
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
        public virtual void Subscribe<T>(Func<T, bool> hander, string queue)
        {
            if (hander == null) throw new ArgumentNullException(nameof(hander));
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            var channel = GetSubscribeChannel();
            lock (lockSubChannel)
            {
                channel.BasicQos(0, 1, false);
                var eventingBasicConsumer = new EventingBasicConsumer(channel);
                eventingBasicConsumer.Received += (o, e) =>
                {
                    var consumer = o as EventingBasicConsumer;
                    bool handerOk = false;
                    try
                    {
                        T m = Deserialize<T>(e.Body);
                        if (m != null) handerOk = hander(m);
                        else handerOk = true;
                    }
                    catch (Exception ex)
                    {
                        try { this.CallbackException?.Invoke(ex, null, string.Empty); }
                        catch { }
                    }

                    if (handerOk)
                    {
                        consumer.Model.BasicAck(e.DeliveryTag, false);
                    }
                    else
                    {
                        consumer.Model.BasicReject(e.DeliveryTag, true);
                    }
                };
                channel.BasicConsume(queue, false, eventingBasicConsumer);
            }
        }

        /// <summary>
        /// 消费消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="hander"></param>
        /// <param name="queue"></param>
        public virtual void AsyncSubscribe<T>(Func<T, Task<bool>> hander, string queue)
        {
            if (hander == null) throw new ArgumentNullException(nameof(hander));
            if (string.IsNullOrEmpty(queue)) throw new ArgumentNullException(nameof(queue));
            var channel = GetSubscribeChannel();
            lock (lockSubChannel)
            {
                channel.BasicQos(0, 1, false);
                var eventingBasicConsumer = new AsyncEventingBasicConsumer(channel);
                eventingBasicConsumer.Received += async (o, e) =>
                {
                    var consumer = o as AsyncEventingBasicConsumer;
                    bool handerOk = false;
                    try
                    {
                        T m = Deserialize<T>(e.Body);
                        if (m != null) handerOk = await hander(m);
                        else handerOk = true;
                    }
                    catch (Exception ex)
                    {
                        try { this.CallbackException?.Invoke(ex, null, string.Empty); }
                        catch { }
                    }

                    if (handerOk)
                    {
                        consumer.Model.BasicAck(e.DeliveryTag, false);
                    }
                    else
                    {
                        consumer.Model.BasicReject(e.DeliveryTag, true);
                    }
                };
                channel.BasicConsume(queue, false, eventingBasicConsumer);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (this.m_subChannel != null) this.m_subChannel.Dispose();
                this.m_subChannel = null;
                IModel model;
                while (this.m_publishChannelQueue.TryDequeue(out model)) model.Dispose();
                this.m_publishChannelQueue = null;
                if (this.m_connection != null) this.m_connection.Dispose();
                this.m_connection = null;
                if (this.delayQueueDic != null) this.delayQueueDic.Clear();
                this.delayQueueDic = null;
                this.CallbackException = null;
                this.lockSubChannel = null;
            }
        }
    }

#if NETCOREAPP || NETSTANDARD
    #region json
    /// <summary>
    /// 
    /// </summary>
    public class StringJsonConverter : JsonConverter<string>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="typeToConvert"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public override string Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Number)
            {
                if (reader.TryGetInt32(out var num))
                    return num.ToString();
                else if (reader.TryGetDecimal(out var dm))
                    return dm.ToString();
            }
            else if (reader.TokenType == JsonTokenType.False || reader.TokenType == JsonTokenType.True)
            {
                return reader.GetBoolean().ToString().ToLower();
            }


            return reader.GetString();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="value"></param>
        /// <param name="options"></param>
        public override void Write(Utf8JsonWriter writer, string value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value);
        }
    }
    /// <summary>
    /// 
    /// </summary>
    public class BooleanJsonConverter : JsonConverter<bool>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="typeToConvert"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public override bool Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Number)
            {
                if (bool.TryParse(reader.GetInt32().ToString(), out var v))
                    return v;
            }
            else if (reader.TokenType == JsonTokenType.String)
            {
                var s = reader.GetString();
                if (s == "on") return true;
                else if (s == "off") return false;
                else if (bool.TryParse(s, out var v))
                    return v;
                else return false;
            }

            return reader.GetBoolean();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="value"></param>
        /// <param name="options"></param>
        public override void Write(Utf8JsonWriter writer, bool value, JsonSerializerOptions options)
        {
            writer.WriteBooleanValue(value);
        }
    }
    /// <summary>
    /// 
    /// </summary>
    public class IntJsonConverter : JsonConverter<int>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="typeToConvert"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public override int Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                int v = 0;
                if (int.TryParse(reader.GetString(), out v))
                    return v;
            }

            return reader.GetInt32();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="value"></param>
        /// <param name="options"></param>
        public override void Write(Utf8JsonWriter writer, int value, JsonSerializerOptions options)
        {
            writer.WriteNumberValue(value);
        }
    }
    /// <summary>
    /// 
    /// </summary>
    public class LongJsonConverter : JsonConverter<long>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="typeToConvert"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public override long Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                long v = 0;
                if (long.TryParse(reader.GetString(), out v))
                    return v;
            }

            return reader.GetInt64();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="value"></param>
        /// <param name="options"></param>
        public override void Write(Utf8JsonWriter writer, long value, JsonSerializerOptions options)
        {
            writer.WriteNumberValue(value);
        }
    }
    /// <summary>
    /// 
    /// </summary>
    public class FloatJsonConverter : JsonConverter<float>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="typeToConvert"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public override float Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                float v = 0;
                if (float.TryParse(reader.GetString(), out v))
                    return v;
            }
            
            return reader.GetSingle();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="value"></param>
        /// <param name="options"></param>
        public override void Write(Utf8JsonWriter writer, float value, JsonSerializerOptions options)
        {
            writer.WriteNumberValue(value);
        }
    }
    /// <summary>
    /// 
    /// </summary>
    public class DoubleJsonConverter : JsonConverter<double>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="typeToConvert"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public override double Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                double v = 0;
                if (double.TryParse(reader.GetString(), out v))
                    return v;
            }

            return reader.GetDouble();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="value"></param>
        /// <param name="options"></param>
        public override void Write(Utf8JsonWriter writer, double value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value.ToString());
        }
    }
    /// <summary>
    /// 
    /// </summary>
    public class DecimalJsonConverter : JsonConverter<decimal>
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="typeToConvert"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public override decimal Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.String)
            {
                decimal v = 0;
                if (decimal.TryParse(reader.GetString(), out v))
                    return v;
            }

            return reader.GetDecimal();
        }
/// <summary>
/// 
/// </summary>
/// <param name="writer"></param>
/// <param name="value"></param>
/// <param name="options"></param>
        public override void Write(Utf8JsonWriter writer, decimal value, JsonSerializerOptions options)
        {
            writer.WriteNumberValue(value);
        }
    }
    #endregion
#endif
}
