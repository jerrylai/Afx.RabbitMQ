using System;
using System.Collections.Generic;
using System.Text;

namespace Afx.RabbitMQ
{
    /// <summary>
    /// mq 应用池接口
    /// </summary>
    public interface IMQPool : IDisposable
    {
        #region Declare
        /// <summary>
        /// ExchangeDeclare
        /// </summary>
        /// <param name="exchange">exchange</param>
        /// <param name="durable">是否持久化</param>
        /// <param name="autoDelete">当已经没有消费者时，服务器是否可以删除该Exchange</param>
        /// <param name="type">direct、fanout、topic</param>
        /// <param name="arguments"></param>
        void ExchangeDeclare(string exchange = "amq.topic", bool durable = true, bool autoDelete = false,
            string type = "topic", IDictionary<string, object> arguments = null);

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
        void QueueDeclare(string queue, string routingKey, bool durable = true, bool exclusive = false, bool autoDelete = false,
             string exchange = "amq.topic", IDictionary<string, object> queueArguments = null, IDictionary<string, object> bindArguments = null);

        /// <summary>
        /// 批量ExchangeDeclare
        /// </summary>
        /// <param name="exchanges"></param>
        void ExchangeDeclare(IEnumerable<ExchangeConfig> exchanges);

        /// <summary>
        /// 批量QueueDeclare
        /// </summary>
        /// <param name="queues"></param>
        void QueueDeclare(IEnumerable<QueueConfig> queues);

        #endregion

        #region
        
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
        bool Publish<T>(T msg, string routingKey, TimeSpan? expire = null,
            string exchange = "amq.topic", bool persistent = false);

        /// <summary>
        /// 发布消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="expire">消息过期时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <returns></returns>
        bool Publish<T>(T msg, PubMsgConfig config, TimeSpan? expire = null, bool persistent = false);

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
        bool Publish<T>(List<T> msgs, string routingKey, TimeSpan? expire = null,
            string exchange = "amq.topic", bool persistent = false);

        /// <summary>
        /// 发布消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msgs">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="expire">消息过期时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <returns></returns>
        bool Publish<T>(List<T> msgs, PubMsgConfig config, TimeSpan? expire = null, bool persistent = false);

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
        bool PublishDelay<T>(T msg, string routingKey, TimeSpan delay,
            string exchange = "amq.topic", bool persistent = false);

        /// <summary>
        /// 发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msg">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <returns>是否发生成功</returns>
        bool PublishDelay<T>(T msg, PubMsgConfig config, TimeSpan delay, bool persistent = false);

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
        bool PublishDelay<T>(List<T> msgs, string routingKey, TimeSpan delay,
            string exchange = "amq.topic", bool persistent = false);

        /// <summary>
        /// 发布延迟消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="msgs">消息</param>
        /// <param name="config">路由配置</param>
        /// <param name="delay">延迟时间</param>
        /// <param name="persistent">消息是否持久化</param>
        /// <returns>是否发生成功</returns>
        bool PublishDelay<T>(List<T> msgs, PubMsgConfig config, TimeSpan delay, bool persistent = false);

        #endregion

        /// <summary>
        /// 消费消息
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="hander"></param>
        /// <param name="queue"></param>
        void Subscribe<T>(Func<T, bool> hander, string queue);
    }
}
