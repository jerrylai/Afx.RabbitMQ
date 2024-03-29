﻿using System;
using System.Collections.Generic;
using System.Text;

namespace Afx.RabbitMQ
{
    /// <summary>
    /// 生产消息配置
    /// </summary>
    public class PubConfig
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
        /// 延迟队列路由key
        /// </summary>
        public string DelayRoutingKey { get; set; }
        /// <summary>
        /// 默认 amq.direct
        /// </summary>
        public string Exchange { get; set; } = "amq.direct";

        /// <summary>
        /// 路由key是否加参数
        /// </summary>
        public bool IsRoutingKeyParam { get; set; } = false;

        /// <summary>
        /// 复制
        /// </summary>
        /// <returns></returns>
        public PubConfig Copy()
        {
            return new PubConfig()
            {
                Name = this.Name,
                Exchange = this.Exchange,
                RoutingKey = this.RoutingKey,
                DelayRoutingKey = this.DelayRoutingKey,
                IsRoutingKeyParam = this.IsRoutingKeyParam
            };
        }
    }
}
