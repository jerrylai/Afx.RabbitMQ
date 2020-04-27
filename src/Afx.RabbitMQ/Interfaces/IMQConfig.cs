using System;
using System.Collections.Generic;
using System.Text;

namespace Afx.RabbitMQ
{
    /// <summary>
    /// mq 配置接口
    /// </summary>
    public interface IMQConfig : IDisposable
    {
        /// <summary>
        /// 获取配置交换器
        /// </summary>
        /// <returns></returns>
        List<ExchangeConfig> GetExchanges();
        /// <summary>
        /// 获取配置队列
        /// </summary>
        /// <returns></returns>
        List<QueueConfig> GetQueues();
        /// <summary>
        /// 获取配置生产者
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        PubMsgConfig GetPubMsgConfig(string name);
        /// <summary>
        /// 获取配置消费者
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        SubMsgConfig GetSubMsgConfig(string name);
    }
}
