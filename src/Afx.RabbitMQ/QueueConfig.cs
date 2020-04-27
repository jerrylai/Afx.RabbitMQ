using System;
using System.Collections.Generic;
using System.Text;

namespace Afx.RabbitMQ
{
    /// <summary>
    /// 队列配置
    /// </summary>
    public class QueueConfig
    {
        /// <summary>
        /// 队列
        /// </summary>
        public string Queue { get; set; }
        /// <summary>
        /// 队列路由key
        /// </summary>
        public string RoutingKey { get; set; }

        /// <summary>
        /// 队列是否持久化，默认true
        /// </summary>
        public bool Durable { get; set; } = true;
        /// <summary>
        /// 连接断开是否删除队列，默认false
        /// </summary>
        public bool Exclusive { get; set; } = false;
        /// <summary>
        /// 当已经没有消费者时，服务器是否可以删除该Exchange， 默认false
        /// </summary>
        public bool AutoDelete { get; set; } = false;

        /// <summary>
        /// 默认amq.topic
        /// </summary>
        public string Exchange { get; set; } = "amq.topic";

        /// <summary>
        /// 队列参数
        /// </summary>
        public IDictionary<string, object> QueueArguments { get; set; }
        /// <summary>
        /// 队列绑定交换器参数
        /// </summary>
        public IDictionary<string, object> BindArguments { get; set; }
        /// <summary>
        /// 路由key是否加参数
        /// </summary>
        public bool IsRoutingKeyParam { get; set; } = false;
        /// <summary>
        /// 队列是否加参数
        /// </summary>
        public bool IsQueueParam { get; set; } = false;
        /// <summary>
        /// 复制
        /// </summary>
        /// <returns></returns>
        public QueueConfig Copy()
        {
            return new QueueConfig()
            {
                Queue = this.Queue,
                RoutingKey = this.RoutingKey,
                Exchange = this.Exchange,
                Durable = this.Durable,
                AutoDelete = this.AutoDelete,
                Exclusive = this.Exclusive,
                QueueArguments = this.QueueArguments == null ? null : new Dictionary<string, object>(this.QueueArguments),
                BindArguments = this.BindArguments == null ? null : new Dictionary<string, object>(this.BindArguments),
                IsRoutingKeyParam = this.IsRoutingKeyParam,
                IsQueueParam = this.IsQueueParam
            };
        }
    }
}
