using System;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using Microsoft.StreamProcessing;
using System.Diagnostics;

namespace Trest
{
    internal class Signal
    {
        public Signal(long ts, float val)
        {
            this.ts = ts;
            this.val = val;
        }

        public long ts;
        public float val;

        public override string ToString()
        {
            return $"ts={this.ts}\tval={this.val}";
        }
    };

    internal class JoinedSignal
    {
        public JoinedSignal(long ts, float ecg, float abp, float po)
        {
            this.ts = ts;
            this.ecg = ecg;
            this.abp = abp;
            this.po = po;
        }

        public long ts;
        public float ecg;
        public float abp;
        public float po;

        public override string ToString()
        {
            return $"ts={this.ts}\tecg={this.ecg}\tabp={this.abp}\tpo={this.po}";
        }
    };

    internal class DataLoader
    {
        public DataLoader(string tag, long size)
        {
            this.size = size;
            this.counter = 0;
            this.lines = File.ReadAllLines(tag);
        }

        private long size;
        private long counter;
        private string[] lines;

        public bool HasNext()
        {
            return this.counter < this.size;
        }

        public void Next()
        {
            this.counter++;
        }

        private Signal ToSignal(string line)
        {
            var fs = line.Split(",");
            return new Signal(this.counter, float.Parse(fs[1]));
        }

        public Signal GetItem()
        {
            return ToSignal(lines[this.counter % this.lines.Length]);
        }
    };

    public sealed class Program
    {
        private static void WriteEvent<T>(StreamEvent<T> e)
        {
            Console.WriteLine($"({e.StartTime}\t" +
                $"{e.EndTime}\t({e.Payload.ToString()}))");
        }

        private static IObservable<Signal> getData(string tag, long size)
        {
            return Observable.Generate(
                new DataLoader($@"E:\{tag}.csv", size),
                d => d.HasNext(),
                d => { d.Next(); return d; },
                d => d.GetItem());
        }

        private static IStreamable<Empty, Signal> getStream(IObservable<Signal> obs)
        {
            return obs.Select(e => StreamEvent.CreateInterval(e.ts, e.ts+1, e))
            //return obs.Select(e => StreamEvent.CreateInterval(StreamEvent.MinSyncTime, StreamEvent.InfinitySyncTime, e))
                .ToStreamable(DisorderPolicy.Drop());
        }

        public const long SIZE = 10000000;

        public static void Main(string[] args)
        {
            Stopwatch sw0 = new Stopwatch();
            sw0.Start();
            var ecgSignalStreamable = getStream(getData("ecg", SIZE));
            var abpSignalStreamable = getStream(getData("abp", SIZE));
            var poSignalStreamable = getStream(getData("po", SIZE));
            sw0.Stop();
            Console.WriteLine("Cache Elapsed = {0}", sw0.Elapsed.TotalSeconds);

            Stopwatch sw = new Stopwatch();
            sw.Start();

            var joinedSignal = ecgSignalStreamable.Join(abpSignalStreamable,
                                    e => e.ts, e => e.ts, (l, r) => new { l.ts, v1 = l.val, v2 = r.val })
                                        .Join(poSignalStreamable, e => e.ts, e => e.ts,
                                            (l, r) => new JoinedSignal(l.ts, l.v1, l.v2, r.val));

            IObservable<StreamEvent<JoinedSignal>> passthroughSignalStreamEventObservable =
                joinedSignal.ToStreamEventObservable();
            passthroughSignalStreamEventObservable
                .Where(e => e.IsData)
                //.ForEachAsync(e => WriteEvent(e))
                .Wait();
            sw.Stop();
            Console.WriteLine("Query Elapsed = {0}\t Throughput = {1}", sw.Elapsed.TotalSeconds, SIZE / sw.Elapsed.TotalSeconds);
        }
    }
}