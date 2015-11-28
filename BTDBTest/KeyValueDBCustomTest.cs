using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using BTDB.KVDBLayer;
using Xunit;
using SnappyCompressionStrategy = BTDB.KVDBLayer.SnappyCompressionStrategy;

namespace BTDBTest
{   
    public class KeyValueDBCustomTest
    {
        public KeyValueDBCustomTest()
        {
            if (Directory.Exists("data"))
            {
                foreach (string file in Directory.EnumerateFiles("data"))
                    File.Delete(file);
            }
            else
                Directory.CreateDirectory("data");
        }

        [Fact(Skip="Takes too long time")]
        public void Reader()
        {
            IEnumerable<KeyValuePair<long, Tick>> flow = TicksGenerator.GetFlow(1000000, KeysType.Random);

            //passed
            //using (var fileCollection = new OnDiskFileCollection("data"))
            //failed
            using (var fileCollection = new OnDiskMemoryMappedFileCollection("data"))
            //passed
            //using (var fileCollection = new InMemoryFileCollection())
            {
                using (IKeyValueDB db = new KeyValueDB(fileCollection, new SnappyCompressionStrategy(), (uint)Int16.MaxValue * 10))
                {
                    using (var tr = db.StartTransaction())
                    {
                        foreach (KeyValuePair<long, Tick> item in flow)
                        {
                            byte[] key = Direct(item.Key);
                            byte[] value = FromTick(item.Value);

                            tr.CreateOrUpdateKeyValue(key, value);
                        }
                        tr.Commit();
                    }

                    flow = TicksGenerator.GetFlow(1000000, KeysType.Random);

                    foreach (KeyValuePair<long, Tick> item in flow)
                    {
                        using (var tr = db.StartTransaction())
                        {
                            byte[] key = Direct(item.Key);
                            bool find = tr.FindExactKey(key);

                            if (find)
                            {
                                var id = Reverse(tr.GetKeyAsByteArray());
                                var tick = ToTick(tr.GetValueAsByteArray());
                                Assert.Equal(item.Key, id);
                            }
                        }
                    }
                }
            }
        }

        byte[] FromTick(Tick tick)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                var writer = new BinaryWriter(stream);

                writer.Write(tick.Symbol);
                writer.Write(tick.Timestamp.Ticks);
                writer.Write(tick.Bid);
                writer.Write(tick.Ask);
                writer.Write(tick.BidSize);
                writer.Write(tick.AskSize);
                writer.Write(tick.Provider);

                return stream.ToArray();
            }
        }

        Tick ToTick(byte[] value)
        {
            var tick = new Tick();

            using (MemoryStream stream = new MemoryStream(value))
            {
                var reader = new BinaryReader(stream);

                tick.Symbol = reader.ReadString();
                tick.Timestamp = new DateTime(reader.ReadInt64());
                tick.Bid = reader.ReadDouble();
                tick.Ask = reader.ReadDouble();
                tick.BidSize = reader.ReadInt32();
                tick.AskSize = reader.ReadInt32();
                tick.Provider = reader.ReadString();
            }

            return tick;
        }

        byte[] Direct(Int64 key)
        {
            var val = (UInt64)(key + Int64.MaxValue + 1);
            var index = BitConverter.GetBytes(val);

            byte[] buf = new byte[8];
            buf[0] = index[7];
            buf[1] = index[6];
            buf[2] = index[5];
            buf[3] = index[4];
            buf[4] = index[3];
            buf[5] = index[2];
            buf[6] = index[1];
            buf[7] = index[0];

            return buf;
        }

        Int64 Reverse(byte[] index)
        {
            byte[] buf = new byte[8];
            buf[0] = index[7];
            buf[1] = index[6];
            buf[2] = index[5];
            buf[3] = index[4];
            buf[4] = index[3];
            buf[5] = index[2];
            buf[6] = index[1];
            buf[7] = index[0];

            UInt64 val = BitConverter.ToUInt64(buf, 0);

            return (Int64)(val - (UInt64)Int64.MaxValue - 1);
        }
    }


    public static class TicksGenerator
    {
        static readonly string[] symbols;
        static readonly int[] digits;
        static readonly double[] pipsizes;
        static readonly double[] prices;
        static readonly string[] providers;

        static TicksGenerator()
        {
            //2013-11-12 13:00
            var data = new string[] { "USDCHF;4;0.9197", "GBPUSD;4;1.5880", "EURUSD;4;1.3403", "USDJPY;2;99.73", "EURCHF;4;1.2324", "AUDBGN;4;1.3596", "AUDCHF;4;0.8567", "AUDJPY;2;92.96", 
                "BGNJPY;2;68.31", "BGNUSD;4;0.6848", "CADBGN;4;1.3901", "CADCHF;4;0.8759", "CADUSD;4;0.9527", "CHFBGN;4;1.5862", "CHFJPY;2;108.44", "CHFUSD;4;1.0875", "EURAUD;4;1.4375", "EURCAD;4;1.4064", 
                "EURGBP;4;0.8438", "EURJPY;4;133.66", "GBPAUD;4;1.7031", "GBPBGN;4;2.3169", "GBPCAD;4;1.6661", "GBPCHF;4;1.4603", "GBPJPY;2;158.37", "NZDUSD;4;0.8217", "USDBGN;4;1.4594", "USDCAD;4;1.0493",
                "XAUUSD;2;1281.15", "XAGUSD;2;21.21", "$DAX;2;9078.20","$FTSE;2;6707.49","$NASDAQ;2;3361.02","$SP500;2;1771.32"};

            symbols = new string[data.Length];
            digits = new int[data.Length];
            pipsizes = new double[data.Length];
            prices = new double[data.Length];

            providers = new string[] { "eSignal", "Gain", "NYSE", "TSE", "NASDAQ", "Euronext", "LSE", "SSE", "ASE", "SE", "NSEI" };

            var format = new NumberFormatInfo();
            format.NumberDecimalSeparator = ".";

            for (int i = 0; i < data.Length; i++)
            {
                var tokens = data[i].Split(';'); //symbol;digits;price

                symbols[i] = tokens[0];
                digits[i] = Int32.Parse(tokens[1]);
                pipsizes[i] = Math.Round(Math.Pow(10, -digits[i]), digits[i]);
                prices[i] = Math.Round(Double.Parse(tokens[2], format), digits[i]);
            }
        }

        public static IEnumerable<KeyValuePair<long, Tick>> GetFlow(long number, KeysType keysType)
        {
            var random = new Random(0);

            //init startup prices
            var prices = TicksGenerator.prices.ToArray();

            DateTime timestamp = DateTime.Now;

            //generate ticks
            for (long i = 0; i < number; i++)
            {
                int id = random.Next(symbols.Length);

                //random movement (Random Walk)
                int direction = random.Next() % 2 == 0 ? 1 : -1;
                int pips = random.Next(0, 10);
                int spread = random.Next(2, 30);
                int seconds = random.Next(1, 30);

                string symbol = symbols[id];
                int d = digits[id];
                double pipSize = pipsizes[id];

                //generate values
                timestamp = timestamp.AddSeconds(seconds);
                double bid = Math.Round(prices[id] + direction * pips * pipSize, d);
                double ask = Math.Round(bid + spread * pipSize, d);
                int bidSize = random.Next(0, 10000);
                int askSize = random.Next(0, 10000);
                string provider = providers[random.Next(providers.Length)];

                //create tick
                var tick = new Tick(symbol, timestamp, bid, ask, bidSize, askSize, provider);

                var key = keysType == KeysType.Sequential ? i : unchecked(random.Next() * i);
                var kv = new KeyValuePair<long, Tick>(key, tick);
                yield return kv;

                prices[id] = bid;
            }
        }
    }

    public enum KeysType : byte
    {
        Sequential,
        Random
    }


    public class Tick : IComparable<Tick>
    {
        public string Symbol { get; set; }
        public DateTime Timestamp { get; set; }
        public double Bid { get; set; }
        public double Ask { get; set; }
        public int BidSize { get; set; }
        public int AskSize { get; set; }
        public string Provider { get; set; }

        public Tick()
        {
        }

        public Tick(string symbol, DateTime time, double bid, double ask, int bidSize, int askSize, string provider)
        {
            Symbol = symbol;
            Timestamp = time;
            Bid = bid;
            Ask = ask;
            BidSize = bidSize;
            AskSize = askSize;
            Provider = provider;
        }

        public override string ToString()
        {
            return String.Format("{0};{1:yyyy-MM-dd HH:mm:ss};{2};{3};{4};{5};{6}", Symbol, Timestamp, Bid, Ask, BidSize, AskSize, Provider);
        }

        public int CompareTo(Tick other)
        {
            if (other.Ask.CompareTo(this.Ask) != 0)
                return -1;
            if (other.AskSize != this.AskSize)
                return -1;
            if (Math.Abs(other.Bid - this.Bid) > 1e-50)
                return -1;
            if (other.BidSize != this.BidSize)
                return -1;
            if (!other.Provider.Equals(this.Provider))
                return -1;
            if (!other.Symbol.Equals(this.Symbol))
                return -1;
            if (!other.Timestamp.Equals(this.Timestamp))
                return -1;
            return 0;
        }
    }
}
