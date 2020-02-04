using System;
using System.Linq;
using System.Data.SQLite;
using iBoxDB.LocalServer;
using System.Threading.Tasks;
using System.Threading;

namespace benchmarksql
{

    class Program
    {

        static int threadCount = 100_000;
        static int batchCount = 10;

        static int reinterationSelect = 3;

        //never set root = "" or "./", when inside a IDE, 
        //the IDE would open the database-files and block writing.
        static String root = "../"; //"/tmp/";

        static void Main(string[] args)
        {
            Console.WriteLine($"ThreadCount={threadCount.ToString("N0")}, batchCount={batchCount}, reinteration={reinterationSelect}");
            Console.WriteLine("iBoxDB");
            TestiBoxDB();

            Console.WriteLine("\r\nSQLite");
            TestSqlite();

            Console.WriteLine("End.");
        }

        public static void TestiBoxDB()
        {
            DB.Root(root);
            long idbFile = 1L;
            BoxSystem.DBDebug.DeleteDBFiles(idbFile);

            using var server = new AppServer();

            using var db = server.GetInstance(idbFile);
            Console.Write("Database Transaction Test: ");
            using var box1 = db.Cube();
            box1["T1"].Insert(new T1 { Id = -1, S = (-1).ToString() });

            using var box2 = db.Cube();
            box2["T1"].Insert(new T1 { Id = -2, S = (-2).ToString() });

            var transaction1 = box1.Select<T1>("from T1").ToArray();
            var transaction2 = box2.Select<T1>("from T1").ToArray();
            if (transaction1.Length == 1 && transaction1[0].Id == -1 &&
                 transaction2.Length == 1 && transaction2[0].Id == -2)
            {
                Console.WriteLine("Succeeded");
            }
            else
            {
                Console.WriteLine("Failed");
            }
            box1.Commit();
            box2.Commit();


            BoxSystem.DBDebug.StartWatch();
            int count = 0;
            Parallel.For(0, threadCount, (p) =>
            {
                using var box = db.Cube();
                for (int i = 0; i < batchCount; i++)
                {
                    var id = (p * batchCount) + i;
                    box["T1"].Insert(new T1 { Id = id, S = id.ToString() });
                    Interlocked.Add(ref count, 1);
                }
                CommitResult cr = box.Commit();

                var minId = p * batchCount + 0;
                var maxId = p * batchCount + batchCount;
                for (int r = 0; r < reinterationSelect; r++)
                    using (var boxt = db.Cube())
                    {
                        var reader = boxt.Select<T1>("from T1 where Id>=? & Id<? order by Id", minId, maxId).GetEnumerator();
                        var ti = minId;
                        while (reader.MoveNext())
                        {
                            var iv = reader.Current.Id;
                            if (ti != iv)
                            {
                                throw new Exception(ti + "  " + iv);
                            }
                            ti++;

                        }
                        if (ti != maxId)
                        {
                            throw new Exception();
                        }
                    }

            }
            );

            if (count != (batchCount * threadCount)) { throw new Exception(count + "  " + (batchCount * threadCount)); }
            var avg = (int)(count / BoxSystem.DBDebug.StopWatch().TotalSeconds);
            Console.WriteLine("iBoxDB Insert:" + count.ToString("N0") + "  AVG: " + avg.ToString("N0") + " objects/s");

            //---------Update-----------------//
            BoxSystem.DBDebug.StartWatch();
            count = 0;
            Parallel.For(0, threadCount, (p) =>
            {
                using var box = db.Cube();
                for (int i = 0; i < batchCount; i++)
                {
                    var id = (p * batchCount) + i;
                    var t = box["T1", id].Update<T1>();
                    t.S = "A" + t.S;
                    Interlocked.Add(ref count, 1);
                }
                CommitResult cr = box.Commit();


                var minId = p * batchCount + 0;
                var maxId = p * batchCount + batchCount;
                using var boxt = db.Cube();
                var reader = boxt.Select<T1>("from T1 where Id>=? & Id<? order by Id", minId, maxId).GetEnumerator();
                var ti = minId;
                while (reader.MoveNext())
                {
                    var iv = reader.Current.Id;
                    if (ti != iv)
                    {
                        throw new Exception(ti + "  " + iv);
                    }
                    if (reader.Current.S != ("A" + iv))
                    {
                        throw new Exception();
                    }
                    ti++;
                }
                if (ti != maxId)
                {
                    throw new Exception();
                }
            }
            );

            if (count != (batchCount * threadCount)) { throw new Exception(count + "  " + (batchCount * threadCount)); }
            avg = (int)(count / BoxSystem.DBDebug.StopWatch().TotalSeconds);
            Console.WriteLine("iBoxDB Update:" + count.ToString("N0") + "  AVG: " + avg.ToString("N0") + " objects/s");

            //-------Delete---------------//
            BoxSystem.DBDebug.StartWatch();
            count = 0;
            Parallel.For(0, threadCount, (p) =>
            {
                using var box = db.Cube();
                for (int i = 0; i < batchCount; i++)
                {
                    var id = (p * batchCount) + i;
                    box["T1", id].Delete();
                    Interlocked.Add(ref count, 1);

                }
                CommitResult cr = box.Commit();

            }
            );

            if (count != (batchCount * threadCount)) { throw new Exception(count + "  " + (batchCount * threadCount)); }
            avg = (int)(count / BoxSystem.DBDebug.StopWatch().TotalSeconds);
            Console.WriteLine("iBoxDB Delete:" + count.ToString("N0") + "  AVG: " + avg.ToString("N0") + " objects/s");

            if (db.Get().SelectCount("from T1") != 2)
            {
                throw new Exception();
            }


        }



        public static void TestSqlite()
        {
            String sdbfile = $"Data Source={root}test.db";
            SQLiteConnection.CreateFile($"{root}test.db");


            using (var con = new SQLiteConnection(sdbfile))
            {
                con.Open();
                using var cmd = new SQLiteCommand(con);
                cmd.CommandText = "DROP TABLE IF EXISTS T1";
                cmd.ExecuteNonQuery();

                cmd.CommandText = @"CREATE TABLE T1(Id INTEGER PRIMARY KEY,
                    S TEXT)";
                cmd.ExecuteNonQuery();
            }

            /* 
                        try
                        {
                            using var con1 = new SQLiteConnection(sdbfile);
                            con1.Open();
                            using var com1 = con1.CreateCommand();
                            com1.Transaction = con1.BeginTransaction();
                            com1.CommandText = "INSERT INTO T1(Id, S) VALUES(-1,'-1')";


                            using var con2 = new SQLiteConnection(sdbfile);
                            con2.Open();
                            using var com2 = con2.CreateCommand();
                            com2.Transaction = con2.BeginTransaction();
                            com2.CommandText = "INSERT INTO T1(Id, S) VALUES(-2,'-2')";

                            com1.ExecuteNonQuery();
                            com2.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.Message);
                        }
            */



            BoxSystem.DBDebug.StartWatch();
            int count = 0;
            Parallel.For(0, threadCount, (p) =>
            {
                using (var con1 = new SQLiteConnection(sdbfile))
                {
                    con1.Open();
                    using var com1 = con1.CreateCommand();
                    com1.Transaction = con1.BeginTransaction();
                    com1.CommandText = "INSERT INTO T1(Id, S) VALUES( @Id , @S )";
                    for (int i = 0; i < batchCount; i++)
                    {
                        var id = (p * batchCount) + i;
                        com1.Parameters.AddWithValue("@Id", id);
                        com1.Parameters.AddWithValue("@S", id.ToString());
                        com1.ExecuteNonQuery();
                        Interlocked.Add(ref count, 1);
                    }

                    com1.Transaction.Commit();
                    com1.Transaction.Dispose();
                }

                var minId = p * batchCount + 0;
                var maxId = p * batchCount + batchCount;
                for (int r = 0; r < reinterationSelect; r++)
                    using (var con2 = new SQLiteConnection(sdbfile))
                    {
                        con2.Open();
                        using var com2 = con2.CreateCommand();
                        com2.CommandText = "select Id, S from T1 where Id>= @minId and Id< @maxId order by Id";
                        com2.Parameters.AddWithValue("@minId", minId);
                        com2.Parameters.AddWithValue("@maxId", maxId);

                        using var reader = com2.ExecuteReader();
                        var ti = minId;
                        while (reader.Read())
                        {
                            var iv = reader.GetInt32(0);
                            if (ti != iv)
                            {
                                throw new Exception(ti + "  " + iv);
                            }
                            ti++;

                        }
                        if (ti != maxId)
                        {
                            throw new Exception();
                        }
                    }

            }
            );

            if (count != (batchCount * threadCount))
            {
                throw new Exception(count + "  " + (batchCount * threadCount));
            }
            var avg = (int)(count / BoxSystem.DBDebug.StopWatch().TotalSeconds);
            Console.WriteLine("SQLite Insert:" + count.ToString("N0") + "  AVG: " + avg.ToString("N0") + " objects/s");


            //---------Update-----------------//
            BoxSystem.DBDebug.StartWatch();
            count = 0;
            Parallel.For(0, threadCount, (p) =>
            {

                using (var con1 = new SQLiteConnection(sdbfile))
                {
                    con1.Open();
                    using var com1 = con1.CreateCommand();
                    com1.Transaction = con1.BeginTransaction();
                    com1.CommandText = "update T1 set S = @S  where Id = @Id";
                    for (int i = 0; i < batchCount; i++)
                    {
                        var id = (p * batchCount) + i;
                        com1.Parameters.AddWithValue("@Id", id);
                        com1.Parameters.AddWithValue("@S", "A" + id);
                        com1.ExecuteNonQuery();
                        Interlocked.Add(ref count, 1);
                    }

                    com1.Transaction.Commit();
                    com1.Transaction.Dispose();
                }


                var minId = p * batchCount + 0;
                var maxId = p * batchCount + batchCount;

                using var con2 = new SQLiteConnection(sdbfile);

                con2.Open();
                using var com2 = con2.CreateCommand();
                com2.CommandText = "select Id, S from T1 where Id>= @minId and Id< @maxId order by Id";
                com2.Parameters.AddWithValue("@minId", minId);
                com2.Parameters.AddWithValue("@maxId", maxId);

                using var reader = com2.ExecuteReader();

                var ti = minId;
                while (reader.Read())
                {
                    var iv = reader.GetInt32(0);
                    if (ti != iv)
                    {
                        throw new Exception(ti + "  " + iv);
                    }
                    if (reader.GetString(1) != ("A" + iv))
                    {
                        throw new Exception();
                    }
                    ti++;
                }
                if (ti != maxId)
                {
                    throw new Exception();
                }
            }
            );

            if (count != (batchCount * threadCount)) { throw new Exception(count + "  " + (batchCount * threadCount)); }
            avg = (int)(count / BoxSystem.DBDebug.StopWatch().TotalSeconds);
            Console.WriteLine("SQLite Update:" + count.ToString("N0") + "  AVG: " + avg.ToString("N0") + " objects/s");


            //-------Delete---------------//
            BoxSystem.DBDebug.StartWatch();
            count = 0;
            Parallel.For(0, threadCount, (p) =>
            {

                using (var con1 = new SQLiteConnection(sdbfile))
                {
                    con1.Open();
                    using var com1 = con1.CreateCommand();
                    com1.Transaction = con1.BeginTransaction();
                    com1.CommandText = "delete from T1  where Id = @Id";
                    for (int i = 0; i < batchCount; i++)
                    {
                        var id = (p * batchCount) + i;
                        com1.Parameters.AddWithValue("@Id", id);
                        com1.ExecuteNonQuery();
                        Interlocked.Add(ref count, 1);
                    }

                    com1.Transaction.Commit();
                    com1.Transaction.Dispose();
                }

            }
            );

            if (count != (batchCount * threadCount)) { throw new Exception(count + "  " + (batchCount * threadCount)); }
            avg = (int)(count / BoxSystem.DBDebug.StopWatch().TotalSeconds);
            Console.WriteLine("SQLite Delete:" + count.ToString("N0") + "  AVG: " + avg.ToString("N0") + " objects/s");


            {
                var stm = "select count(*) from T1";
                using var con = new SQLiteConnection(sdbfile);
                con.Open();
                var cmd = new SQLiteCommand(stm, con);
                if (cmd.ExecuteScalar().ToString() != "0")
                {
                    throw new Exception();
                }
            }
        }

        public class T1
        {
            public T1() { }
            public int Id { get; set; }
            public string S;
        }

        class AppServer : LocalDatabaseServer
        {
            public class C1Config : iBoxDB.LocalServer.IO.BoxFileStreamConfig
            {
                public C1Config()
                {
                    this.CacheLength = MB(512L);
                    this.EnsureTable<T1>("T1", "Id");
                }
            }

            protected override DatabaseConfig BuildDatabaseConfig(long addr)
            {
                return new C1Config();
            }
        }
    }
}