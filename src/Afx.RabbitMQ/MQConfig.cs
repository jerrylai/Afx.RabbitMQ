using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;
using System.Xml;

namespace Afx.RabbitMQ
{
    /// <summary>
    /// mq配置
    /// </summary>
    public class MQConfig : IMQConfig
    {
        private List<ExchangeConfig> exchangeList;
        private List<QueueConfig> queueList;
        private Dictionary<string, PubConfig> pubMsgDic;
        private Dictionary<string, SubConfig> subMsgDic;

        /// <summary>
        /// mq配置
        /// </summary>
        /// <param name="xmlFile"></param>
        public MQConfig(string xmlFile)
        {
            if (string.IsNullOrEmpty(xmlFile)) throw new ArgumentNullException(nameof(xmlFile));
            if (!System.IO.File.Exists(xmlFile)) throw new System.IO.FileNotFoundException(nameof(xmlFile), xmlFile);
            this.Load(xmlFile);
        }

        private void Load(string xmlFile)
        {
            using (var fs = System.IO.File.Open(xmlFile, System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite))
            {
                XmlReaderSettings settings = new XmlReaderSettings() { IgnoreComments = true, XmlResolver = null };
                using (var rd = XmlReader.Create(fs, settings))
                {
                    XmlDocument xmlDocument = new XmlDocument();
                    xmlDocument.XmlResolver = null;
                    xmlDocument.Load(rd);

                    if(xmlDocument.DocumentElement == null) throw new ArgumentException($"MQ config is null!");

                    this.LoadExchangeConfig(xmlDocument.DocumentElement);
                    this.LosdQueueConfig(xmlDocument.DocumentElement);
                    this.LoadPubConfig(xmlDocument.DocumentElement);
                    this.LoadSubConfig(xmlDocument.DocumentElement);
                }
            }
        }

        private void LoadExchangeConfig(XmlElement rootElement)
        {
            var nodes = rootElement.SelectNodes("Exchange/Key");
            if (nodes == null || nodes.Count == 0) nodes = rootElement.SelectNodes("ExchangeConfig/Key");
            exchangeList = new List<ExchangeConfig>(nodes.Count);
            foreach (XmlNode node in nodes)
            {
                if (node is XmlElement)
                {
                    XmlElement element = node as XmlElement;
                    string s = element.GetAttribute("exchange");
                    if (string.IsNullOrEmpty(s)) throw new ArgumentException("Exchange config is null!");
                    if (exchangeList.Exists(q => q.Exchange == s)) throw new ArgumentException($"Exchange config ({s}) is repeat！");

                    var m = new ExchangeConfig() { Exchange = s };
                    s = element.GetAttribute("type");
                    if (!string.IsNullOrEmpty(s)) m.Type = s;
                    s = element.GetAttribute("durable");
                    if (!string.IsNullOrEmpty(s)) m.Durable = s.ToLower() == "true" || s == "1";
                    s = element.GetAttribute("autoDelete");
                    if (!string.IsNullOrEmpty(s)) m.AutoDelete = s.ToLower() == "true" || s == "1";

                    var args = node.SelectNodes("Arguments");
                    m.Arguments = new Dictionary<string, object>(args.Count);
                    foreach (var rnode in args)
                    {
                        if (rnode is XmlElement)
                        {
                            var rel = rnode as XmlElement;
                            var k = rel.GetAttribute("key");
                            var v = rel.GetAttribute("value");
                            if (!string.IsNullOrEmpty(k) && !string.IsNullOrEmpty(v))
                            {
                                m.Arguments[k] = v;
                            }
                        }
                    }

                    exchangeList.Add(m);
                }
            }
            exchangeList.TrimExcess();
        }

        private void LosdQueueConfig(XmlElement rootElement)
        {
            var nodes = rootElement.SelectNodes("Queue/Key");
            if (nodes == null || nodes.Count == 0) nodes = rootElement.SelectNodes("QueueConfig/Key");
            queueList = new List<QueueConfig>(nodes.Count);
            foreach (XmlNode node in nodes)
            {
                if (node is XmlElement)
                {
                    XmlElement element = node as XmlElement;
                    string s = element.GetAttribute("queue");
                    if (string.IsNullOrEmpty(s)) throw new ArgumentException("Queue config is null!");
                    if (queueList.Exists(q => q.Queue == s)) throw new ArgumentException($"Queue config ({s}) is repeat！");

                    var m = new QueueConfig() { Queue = s };
                    s = element.GetAttribute("routingKey");
                    m.RoutingKey = s ?? string.Empty;

                    //delay
                    s = element.GetAttribute("delayQueue");
                    if (!string.IsNullOrEmpty(s))
                    {
                        m.DelayQueue = s;
                       s = element.GetAttribute("delayRoutingKey");
                        m.DelayRoutingKey = s ?? string.Empty;
                    }

                    s = element.GetAttribute("exchange");
                    if (!string.IsNullOrEmpty(s)) m.Exchange = s;

                    s = element.GetAttribute("durable");
                    if (!string.IsNullOrEmpty(s)) m.Durable = s.ToLower() == "true" || s == "1";
                    s = element.GetAttribute("exclusive");
                    if (!string.IsNullOrEmpty(s)) m.Exclusive = s.ToLower() == "true" || s == "1";
                    s = element.GetAttribute("autoDelete");
                    if (!string.IsNullOrEmpty(s)) m.AutoDelete = s.ToLower() == "true" || s == "1";

                    s = element.GetAttribute("isQueueParam");
                    if (!string.IsNullOrEmpty(s)) m.IsQueueParam = s.ToLower() == "true" || s == "1";

                    s = element.GetAttribute("isRoutingKeyParam");
                    if (!string.IsNullOrEmpty(s)) m.IsRoutingKeyParam = s.ToLower() == "true" || s == "1";

                    var args = node.SelectNodes("QueueArguments");
                    m.QueueArguments = new Dictionary<string, object>(args.Count);
                    foreach (var rnode in args)
                    {
                        if (rnode is XmlElement)
                        {
                            var rel = rnode as XmlElement;
                            var k = rel.GetAttribute("key");
                            var v = rel.GetAttribute("value");
                            if (!string.IsNullOrEmpty(k) && !string.IsNullOrEmpty(v))
                            {
                                m.QueueArguments[k] = v;
                            }
                        }
                    }

                    args = node.SelectNodes("BindArguments");
                    m.BindArguments = new Dictionary<string, object>(args.Count);
                    foreach (var rnode in args)
                    {
                        if (rnode is XmlElement)
                        {
                            var rel = rnode as XmlElement;
                            var k = rel.GetAttribute("key");
                            var v = rel.GetAttribute("value");
                            if (!string.IsNullOrEmpty(k) && !string.IsNullOrEmpty(v))
                            {
                                m.BindArguments[k] = v;
                            }
                        }
                    }

                    queueList.Add(m);
                }
            }
            queueList.TrimExcess();
        }

        private void LoadPubConfig(XmlElement rootElement)
        {
            var nodes = rootElement.SelectNodes("Pub/Key");
            if(nodes == null || nodes.Count == 0) nodes = rootElement.SelectNodes("PubMsg/Key");
            if (nodes == null || nodes.Count == 0) nodes = rootElement.SelectNodes("PubConfig/Key");
            pubMsgDic = new Dictionary<string, PubConfig>(nodes.Count);
            foreach (XmlNode node in nodes)
            {
                if (node is XmlElement)
                {
                    XmlElement element = node as XmlElement;
                    string s = element.GetAttribute("name");
                    if (string.IsNullOrEmpty(s)) throw new ArgumentNullException("PubMsg config is null!");
                    if (pubMsgDic.ContainsKey(s)) throw new ArgumentException($"PubMsg config ({s}) is repeat！");

                    var m = new PubConfig() { Name = s };
                    s = element.GetAttribute("routingKey");
                    m.RoutingKey = s ?? string.Empty;

                    s = element.GetAttribute("delayRoutingKey");
                    if (!string.IsNullOrEmpty(s)) m.DelayRoutingKey = s;

                    s = element.GetAttribute("exchange");
                    if (!string.IsNullOrEmpty(s)) m.Exchange = s;

                    s = element.GetAttribute("isRoutingKeyParam");
                    if (!string.IsNullOrEmpty(s)) m.IsRoutingKeyParam = s.ToLower() == "true" || s == "1";

                    pubMsgDic.Add(m.Name, m);
                }
            }
        }

        private void LoadSubConfig(XmlElement rootElement)
        {
            var nodes = rootElement.SelectNodes("Sub/Key");
            if (nodes == null || nodes.Count == 0) nodes = rootElement.SelectNodes("SubMsg/Key");
            if (nodes == null || nodes.Count == 0) nodes = rootElement.SelectNodes("SubConfig/Key");
            subMsgDic = new Dictionary<string, SubConfig>(nodes.Count);
            foreach (XmlNode node in nodes)
            {
                if (node is XmlElement)
                {
                    XmlElement element = node as XmlElement;
                    string s = element.GetAttribute("name");
                    if (string.IsNullOrEmpty(s)) throw new ArgumentNullException("SubMsg config is null!");
                    if (subMsgDic.ContainsKey(s)) throw new ArgumentException($"SubMsg config ({s}) is repeat！");

                    var m = new SubConfig() { Name = s };
                    s = element.GetAttribute("queue");
                    if (string.IsNullOrEmpty(s)) throw new ArgumentNullException($"SubMsg ({s}) queue is null!");
                    m.Queue = s;

                    s = element.GetAttribute("isQueueParam");
                    if (!string.IsNullOrEmpty(s)) m.IsQueueParam = s.ToLower() == "true" || s == "1";

                    subMsgDic.Add(m.Name, m);
                }
            }
        }

        /// <summary>
        /// 获取配置交换器
        /// </summary>
        /// <returns></returns>
        public List<ExchangeConfig> GetExchanges()
        {
            return this.exchangeList.Select(q => q.Copy()).ToList();
        }
        /// <summary>
        /// 获取配置队列
        /// </summary>
        /// <returns></returns>
        public List<QueueConfig> GetQueues()
        {
            return queueList.Select(q => q.Copy()).ToList();
        }

        /// <summary>
        /// 获取配置生产消息配置
        /// </summary>
        /// <param name="name">配置name</param>
        /// <returns></returns>
        public PubConfig GetPubConfig(string name)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            PubConfig m = null;

            return pubMsgDic.TryGetValue(name, out m) ? m.Copy() : null;
        }
        /// <summary>
        /// 获取消费配置
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public SubConfig GetSubConfig(string name)
        {
            if (string.IsNullOrEmpty(name)) throw new ArgumentNullException(nameof(name));
            SubConfig m = null;

            return subMsgDic.TryGetValue(name, out m) ? m.Copy() : null;
        }

        /// <summary>
        /// 释放所有资源
        /// </summary>
        public void Dispose()
        {
            this.exchangeList = null;
            this.queueList = null;
            this.pubMsgDic = null;
            this.subMsgDic = null;
        }
    }
}
