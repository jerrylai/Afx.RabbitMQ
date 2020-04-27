using System;
using System.Collections.Generic;
using System.Text;

namespace Afx.RabbitMQ
{
    /// <summary>
    /// 定义交换器参数
    /// </summary>
    public class ExchangeConfig
    {
        /// <summary>
        /// 默认amq.topic
        /// </summary>
        public string Exchange { get; set; } = "amq.topic";
        /// <summary>
        /// direct、fanout、topic, 默认topic
        /// </summary>
        public string Type { get; set; } = "topic";
        /// <summary>
        /// 是否持久化, 默认true
        /// </summary>
        public bool Durable { get; set; } = true;
        /// <summary>
        /// 当已经没有消费者时，服务器是否可以删除该Exchange, 默认false
        /// </summary>
        public bool AutoDelete { get; set; } = false;

        /// <summary>
        /// 参数
        /// </summary>
        public IDictionary<string, object> Arguments { get; set; }

        /// <summary>
        /// 复制副本
        /// </summary>
        /// <returns></returns>
        public ExchangeConfig Copy()
        {
            return new ExchangeConfig()
            {
                Exchange = this.Exchange,
                Type = this.Type,
                Durable = this.Durable,
                AutoDelete = this.AutoDelete,
                Arguments = this.Arguments == null ? null : new Dictionary<string, object>(this.Arguments)
            };
        }
    }
}
