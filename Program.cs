using Aerospike.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace asdelete
{
    class Program
    {
        static AerospikeClient client;
        static int count, total;
        static DateTime oldTime;
        static int limitDeletes;
        static bool verbose;

        static int Main(string[] args)
        {
            if (args.Contains("-verbose"))
            {
                verbose = true;
                args = args.Where(a => a != "-verbose").ToArray();
            }
            else
            {
                verbose = false;
            }

            int port, days, limit;

            if (args.Length != 6 || !int.TryParse(args[1], out port) || !int.TryParse(args[4], out days) || !int.TryParse(args[5], out limit))
            {
                Console.WriteLine(
@"Tool for pre-empty deletion of Aerospike objects that will soon be deleted due to their TTL.

Version 1.0

Usage: asdelete <host> <port> <namespace> <set> <days> <limit>

host:       Aerospike server.
port:       Aeropsike port.
namespace:  Aeropsike namespace.
set:        Aeropsike data set name.
days:       Days into the future - should be a *positive* integer.
limit:      Number of objects to delete. Specify 0 to just perform a count.");
                return 1;
            }

            Delete(args[0], port, args[2], args[3], days, limit);

            return 0;
        }

        static void Delete(string host, int port, string asnamespace, string set, int days, int limit)
        {
            Console.WriteLine($"Host: {host}, Port: {port}, Namespace: {asnamespace}, Set: {set}, Days: {days}, Limit: {limit}");

            oldTime = DateTime.UtcNow.AddDays(days);
            Console.WriteLine("Date: " + oldTime);

            limitDeletes = limit;
            count = total = 0;

            try
            {
                client = new AerospikeClient(host, port);

                ScanPolicy scanPolicy = new ScanPolicy();
                // Scan the entire Set using ScanAll(). This will scan each node
                // in the cluster and return the record Digest to the call back object
                client.ScanAll(scanPolicy, asnamespace, set, ScanCallback, new string[] { });

                Log("Deleted " + count + " records from set " + set);
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

            total++;

            DateTime dt = ASTimeToUtcDateTime(record.expiration);
            if (dt < oldTime)
            {
                if (count < limitDeletes)
                {
                    if (verbose)
                    {
                        Log("Expiration " + dt);
                    }
                    //var token = new CancellationToken();
                    client.Delete(new WritePolicy(), key);
                }

                count++;
                if (count % 10000 == 0)
                {
                    int percent = count * 100 / total;
                    Console.WriteLine($"Count: {count}/{total} ({percent}%)");
                }
            }
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
