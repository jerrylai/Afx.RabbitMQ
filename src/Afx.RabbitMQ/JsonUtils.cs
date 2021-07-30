using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

#if NETCOREAPP || NETSTANDARD
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
#else
using Newtonsoft.Json;
#endif

namespace Afx.RabbitMQ.Json
{
    /// <summary>
    /// json 序列化相关 Utils
    /// </summary>
    internal static class JsonUtils
    {
#if NETCOREAPP || NETSTANDARD
        private static readonly JsonSerializerOptions options = new JsonSerializerOptions()
        {
            IgnoreNullValues = true,
            WriteIndented = true,
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            PropertyNameCaseInsensitive = false,
            PropertyNamingPolicy = null,
            DictionaryKeyPolicy = null
        };

        static JsonUtils()
        {
            options.Converters.Add(new StringJsonConverter());
            options.Converters.Add(new BooleanJsonConverter());
            options.Converters.Add(new IntJsonConverter());
            options.Converters.Add(new LongJsonConverter());
            options.Converters.Add(new FloatJsonConverter());
            options.Converters.Add(new DoubleJsonConverter());
            options.Converters.Add(new DecimalJsonConverter());
        }
#else
        private static readonly JsonSerializerSettings options = new JsonSerializerSettings()
        {
            NullValueHandling = NullValueHandling.Ignore,
            MissingMemberHandling = Newtonsoft.Json.MissingMemberHandling.Ignore
        };
#endif

        /// <summary>
        /// 序列化json
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string Serialize<T>(T value)
        {
            if (value == null) return null;
#if NETCOREAPP || NETSTANDARD
            return JsonSerializer.Serialize(value, options);
#else
            return JsonConvert.SerializeObject(value, options);
#endif
        }

        /// <summary>
        /// 反序列化
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="json"></param>
        /// <returns></returns>
        public static T Deserialize<T>(string json)
        {
            T m = default(T);
            if (string.IsNullOrEmpty(json)) return m;
#if NETCOREAPP || NETSTANDARD
            m = JsonSerializer.Deserialize<T>(json, options);
#else
            m =  JsonConvert.DeserializeObject<T>(json, options);
#endif
            return m;
        }
    }

#if NETCOREAPP || NETSTANDARD
    /// <summary>
    /// 日期格式
    /// </summary>
    internal class DateTimeJsonConverter : JsonConverter<DateTime>
    {
        /// <summary>
        /// 格式
        /// </summary>
        public string Format { get; }
       /// <summary>
       /// 
       /// </summary>
       /// <param name="format"></param>
        public DateTimeJsonConverter(string format)
        {
            this.Format = format;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="typeToConvert"></param>
        /// <param name="options"></param>
        /// <returns></returns>
        public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            var t = DateTime.Parse(reader.GetString());
            if (t.Kind == DateTimeKind.Unspecified)
            {
                t = new DateTime(t.Ticks, DateTimeKind.Local);
            }

            return t;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="value"></param>
        /// <param name="options"></param>
        public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
        {
            writer.WriteStringValue(value.ToString(this.Format));
        }
    }
    /// <summary>
    /// 字符串
    /// </summary>
    internal class StringJsonConverter : JsonConverter<string>
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
    internal class BooleanJsonConverter : JsonConverter<bool>
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
    internal class IntJsonConverter : JsonConverter<int>
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
    internal class LongJsonConverter : JsonConverter<long>
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
    internal class FloatJsonConverter : JsonConverter<float>
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
    internal class DoubleJsonConverter : JsonConverter<double>
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
            writer.WriteNumberValue(value);
        }
    }
    /// <summary>
    /// 
    /// </summary>
    internal class DecimalJsonConverter : JsonConverter<decimal>
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
#endif
}
