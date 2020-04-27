using System;
using System.Collections.Generic;
using System.Text;

namespace Afx.RabbitMQ
{
    /// <summary>
    /// 消费配置
    /// </summary>
    public class SubMsgConfig
    {
        /// <summary>
        /// 消息名称
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// 订阅队列
        /// </summary>
        public string Queue { get; set; }
        /// <summary>
        /// 队列是否加参数
        /// </summary>
        public bool IsQueueParam { get; set; } = false;

        /// <summary>
        /// 复制
        /// </summary>
        /// <returns></returns>
        public SubMsgConfig Copy()
        {
            return new SubMsgConfig()
            {
                Name = this.Name,
                Queue = this.Queue,
                IsQueueParam = this.IsQueueParam
            };
        }
    }
}
