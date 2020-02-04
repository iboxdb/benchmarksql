# benchmarksql
Benchmark with SQLite


### Test Result:

**VM 2cores + 8G**

**iBoxDB.NET v2.21**

**SQLite.Core**

```sh
dotnet run -c Release
```

```sql
ThreadCount=100,000 BatchCount=10 
 
iBoxDB
Database Transaction Test: Succeeded
iBoxDB Insert:1,000,000  AVG: 42,669 objects/s
iBoxDB Update:1,000,000  AVG: 16,251 objects/s
iBoxDB Delete:1,000,000  AVG: 30,216 objects/s

SQLite
SQLite Insert:1,000,000  AVG: 1,497 objects/s
SQLite Update:1,000,000  AVG: 1,177 objects/s
SQLite Delete:1,000,000  AVG: 3,020 objects/s

```


[Test Source Code](https://github.com/iboxdb/benchmarksql/blob/master/Program.cs)

