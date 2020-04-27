using System;
using System.Collections.Generic;
using System.Text;

namespace Afx.RabbitMQ
{
    /// <summary>
    /// 生产消息配置
    /// </summary>
    public class PubMsgConfig
    {
        /// <summary>
        /// 消息名称
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// 路由key
        /// </summary>
        public string RoutingKey { get; set; }

        /// <summary>
        /// 默认 amq.topic
        /// </summary>
        public string Exchange { get; set; } = "amq.topic";

        /// <summary>
        /// 路由key是否加参数
        /// </summary>
        public bool IsRoutingKeyParam { get; set; } = false;

        /// <summary>
        /// 复制
        /// </summary>
        /// <returns></returns>
        public PubMsgConfig Copy()
        {
            return new PubMsgConfig()
            {
                Name = this.Name,
                Exchange = this.Exchange,
                RoutingKey = this.RoutingKey,
                IsRoutingKeyParam = this.IsRoutingKeyParam
            };
        }
    }
}
