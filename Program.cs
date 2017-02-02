using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace asdelete
{
    class Program
    {
        static AerospikeClient _client;
        static long _count, _rewritecount, _total;
        static int _thresholdTime;
        static long _deleteLimit;
        static long _deleteRangeStart;
        static long _deleteRangeEnd;
        static bool _verbose;
        static Stopwatch _watch;

        static int Main(string[] args)
        {
            if (args.Contains("-verbose"))
            {
                _verbose = true;
                args = args.Where(a => a != "-verbose").ToArray();
            }
            else
            {
                _verbose = false;
            }

            int port, days;
            long limit, rangeStart = -1, rangeEnd = -1;

            if (
                (args.Length != 6 || !int.TryParse(args[1], out port) || !int.TryParse(args[4], out days) || !long.TryParse(args[5], out limit))
                &&
                (args.Length != 8 || !int.TryParse(args[1], out port) || !int.TryParse(args[4], out days) || !long.TryParse(args[5], out limit)
                    || !long.TryParse(args[6], out rangeStart) || !long.TryParse(args[7], out rangeEnd))
                )
            {
                Console.WriteLine(
@"Tool for pre-empty deletion of Aerospike objects that will soon be deleted due to their TTL.

Version 1.2

Usage: asdelete [-verbose] <host> <port> <namespace> <set> <days> <limit> [rangestart] [rangeend]

-verbose:   Print expiration time for all objects that are deleted.

host:       Aerospike server.
port:       Aeropsike port.
namespace:  Aeropsike namespace.
set:        Aeropsike data set name.
days:       Days into the future - should be a *positive* integer.
limit:      Maximum number of objects to delete.Specify 0 to just perform a count.
rangestart: Lower bound of date range (exclusive). Optional.
rangeend:   Upper bound of date range (inclusive). Optional.");
                return 1;
            }

            Delete(args[0], port, args[2], args[3], days, limit, rangeStart, rangeEnd);

            return 0;
        }

        static void Delete(string host, int port, string asnamespace, string set, int days, long limit, long rangeStart, long rangeEnd)
        {
            Console.WriteLine($"Host: {host}, Port: {port}, Namespace: {asnamespace}, Set: {set}, Days: {days}, Limit: {limit}, RangeStart: {rangeStart}, RangeEnd: {rangeEnd}");

            DateTime dt = DateTime.UtcNow.AddDays(days);
            Console.WriteLine($"Date: {dt}");

            _thresholdTime = UtcDateTimeToASTime(dt);


            _deleteLimit = limit;
            _deleteRangeStart = rangeStart;
            _deleteRangeEnd = rangeEnd;
            _count = _rewritecount = _total = 0;

            try
            {
                _client = new AerospikeClient(host, port);

                ScanPolicy scanPolicy = new ScanPolicy();
                // Scan the entire Set using ScanAll(). This will scan each node
                // in the cluster and return the record Digest to the call back object

                _watch = Stopwatch.StartNew();
                _client.ScanAll(scanPolicy, asnamespace, set, ScanCallback, new string[] { });
                _watch.Stop();

                Log($"Deleted {_count} records from set {set}. Rewrites: {_rewritecount}");
            }
            catch (AerospikeException ex)
            {
                int resultCode = ex.Result;
                Log(ResultCode.GetResultString(resultCode));
                Log("Error details: " + ex.ToString());
            }

            return;
        }

        public static void ScanCallback(Key key, Record record)
        {
            // For each Digest returned, delete it using Delete()

            _total++;

            if (record.expiration < _thresholdTime ||
                (record.expiration > _deleteRangeStart && record.expiration <= _deleteRangeEnd))
            {
                if (_count < _deleteLimit)
                {
                    if (_verbose)
                    {
                        Log("Expiration: " + ASTimeToUtcDateTime(record.expiration));
                    }

                    try
                    {
                        //var firstbinkey = record.bins.Keys.FirstOrDefault();
                        string firstbinkey = null;
                        if (firstbinkey == null)
                        {
                            if (_verbose)
                            {
                                Log("Using delete.");
                            }
                            _client.Delete(new WritePolicy(), key);
                        }
                        else
                        {
                            if (_verbose)
                            {
                                Log("Using rewrite.");
                            }
                            WritePolicy wp = new WritePolicy();
                            wp.expiration = 1;
                            record.bins[firstbinkey] = record.bins[firstbinkey];
                            _rewritecount++;
                        }
                    }
                    catch (AerospikeException ex)
                    {
                        Log(ex.ToString());
                        return;
                    }
                }

                _count++;
                if (_count % 10000 == 0)
                {
                    long percent = _count * 100 / _total;
                    long rate = _count * 1000 / _watch.ElapsedMilliseconds;
                    Console.WriteLine($"Count: {_count}/{_total} ({percent}%) ({_rewritecount} deletes was rewrites). Current rate: {rate}/s.");
                    _watch = Stopwatch.StartNew();
                }
            }
        }

        static int UtcDateTimeToASTime(DateTime dt)
        {
            TimeSpan ts = dt - new DateTime(2010, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);

            return (int)ts.TotalSeconds;
        }

        static DateTime ASTimeToUtcDateTime(double asTime)
        {
            DateTime dt = new DateTime(2010, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            dt = dt.AddSeconds(asTime);
            return dt;
        }

        static void Log(string message)
        {
            Console.WriteLine(message);
        }
    }
}
