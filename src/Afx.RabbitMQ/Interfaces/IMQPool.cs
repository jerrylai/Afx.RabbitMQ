using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
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
    /// 订阅消息处理
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="m"></param>
    /// <param name="headers"></param>
    /// <returns></returns>
    public delegate bool SubscribeHander<T>(T m, IDictionary<string, object> headers);

    /// <summary>
    /// 订阅消息处理
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="m"></param>
    /// <param name="headers"></param>
    /// <returns></returns>
    public delegate Task<bool> AsyncSubscribeHander<T>(T m, IDictionary<string, object> headers);

    /// <summary>
    /// mq 应用池接口
    /// </summary>
    public interface IMQPool : IDisposable
    {
#if NETCOREAPP || NETSTANDARD
        /// <summary>
        /// 
        /// </summary>
        /// <param name="jsonSerializerOptions"></param>
        void SetJsonOptions(JsonSerializerOptions jsonSerializerOptions);
#else
        /// <summary>
        /// 
        /// </summary>
        /// <param name="jsonSerializerOptions"></param>
        void SetJsonOptions(JsonSerializerSettings jsonSerializerOptions);
#endif
        
        /// <summary>
        /// Returns true if the connection is still in a state where it can be used. Identical
        /// to checking if RabbitMQ.Client.IConnection.CloseReason equal null.
        /// </summary>
        bool IsOpen { get; }
        /// <summary>
        /// The current heartbeat setting for this connection (System.TimeSpan.Zero for disabled).
        /// </summary>
        TimeSpan Heartbeat { get; }

        #region Exchange
        /// <summary>
        /// 
        /// </summary>
        /// <param name="exchange"></param>
        /// <param name="type"></param>
        /// <param name="durable">是否持久化, 默认true</param>
        /// <param name="autoDelete">当已经没有消费者时，服务器是否可以删除该Exchange, 默认false</param>
        /// <param name="arguments"></param>
        void ExchangeDeclare(string exchange = "amq.direct", string type = "direct", bool durable = true, bool autoDelete = false, IDictionary<string, object> arguments = null);

        /// <summary>
        /// ExchangeDeclare
        /// </summary>
        /// <param name="config"></param>
        void ExchangeDeclare(ExchangeConfig config);

        /// <summary>
        /// 批量 ExchangeDeclare
        /// </summary>
        /// <param name="configs"></param>
        void ExchangeDeclare(IEnumerable<ExchangeConfig> configs);

        #endregion

        #region Queue
        /// <summary>
        /// QueueDeclare
        /// </summary>
        /// <param name="config"></param>
        void QueueDeclare(QueueConfig config);

        

        /// <summary>
        /// 批量QueueDeclare
        /// </summary>
        /// <param name="queues"></param>
        void QueueDeclare(IEnumerable<QueueConfig> queues);
        #endregion


        #region Publish

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
        bool Publish<T>(T msg, string routingKey, TimeSpan? expire = null,
            string exchange = "amq.direct", bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null);

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
        bool Publish<T>(T msg, PubMsgConfig config, TimeSpan? expire = null, bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null);

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
        bool Publish<T>(List<T> msgs, string routingKey, TimeSpan? expire = null,
            string exchange = "amq.direct", bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null);

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
        bool Publish<T>(List<T> msgs, PubMsgConfig config, TimeSpan? expire = null, bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null);

        /// <summary>
        /// 发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg">消息</param>
        /// <param name="delayRoutingKey">delayRoutingKey</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="exchange">exchange</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <param name="serialize">自定义序列化</param>
        /// <returns>是否发生成功</returns>
        bool PublishDelay<T>(T msg, string delayRoutingKey, TimeSpan delay,
            string exchange = "amq.direct", bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null);

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
        bool PublishDelay<T>(T msg, PubMsgConfig config, TimeSpan delay, bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null);

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
        bool PublishDelay<T>(List<T> msgs, string delayRoutingKey, TimeSpan delay,
            string exchange = "amq.direct", bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null);

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
        bool PublishDelay<T>(List<T> msgs, PubMsgConfig config, TimeSpan delay, bool persistent = false, Func<T, ReadOnlyMemory<byte>> serialize = null);

        #endregion

        #region Subscribe
        /// <summary>
        /// 同步消费消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="hander"></param>
        /// <param name="queue"></param>
        /// <param name="autoAck">是否自动确认</param>
        /// <param name="deserialize">自定义反序列化</param>
        void Subscribe<T>(SubscribeHander<T> hander, string queue, bool autoAck = false, Func<ReadOnlyMemory<byte>, T> deserialize = null);

        /// <summary>
        /// 异步消费消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="hander"></param>
        /// <param name="queue"></param>
        /// <param name="autoAck">是否自动确认</param>
        /// <param name="deserialize">自定义反序列化</param>
        void Subscribe<T>(AsyncSubscribeHander<T> hander, string queue, bool autoAck = false, Func<ReadOnlyMemory<byte>, T> deserialize = null);
    #endregion
    }
}
