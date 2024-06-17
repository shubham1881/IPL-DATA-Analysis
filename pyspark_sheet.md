

  Agenda / Curriculum
  -------------------

    Spark basics & architecture
    Spark Core API (RDD & Shared Variables)
    RDD Transformations and Actions
    Spark SQL - Basic Concepts
    DataFrame Transformations - DataFrame API & SQL
    Machine Learning Basics & Spark MLlib (Basics)
    Spark Streaming (Introduction)
   --------------------------------------------------------
   
    Big Data : Because of some charecteristics of the data, that data can not
-> Stored on single server based systems
-> Can not be processed using single server based hardware or software.

      1. Volume
2. Velocity (Speed at which data is generated)
3. Variety (Structured, Unstructured and Semi-Structured)


    Cluster
-> A cluster pools the resources of many machines together allowing
  us to use all the cumulative resources as if they were one.

-> A group of machines (nodes) whose cumulative resources are used to
  distribute the storage and processing of your computational workloads.

-> Unified entity
-> Distribute storage across many machines
-> Distribute processing across many machine


   Hadoop is a franework that provides the cluster computing frameworks to solve the
   big data problem associated with storage and processing of big data.

   Hadoop has :

-> Distributed Storage Framework   : HDFS
-> Resource Management Framework   : YARN  (cluster manager)
-> Distributed Computing Framework : MapReduce (2004/2005 upto 2013)

   Disadvantages of Map Reduce:

-> MapReduce uses disk-based computation which is slow.
disk-based : intermediate results of each task are stored on the
    disk. And sensequent tasks read the data from disk.
    (involves lot of disk-io)

-> Is not good with a lot of small files.
-> Not very good with iterative computations
-> Not a flexible programming model


   What is Apache Spark ?
   --------------------

     => Spark is a unified in-memory distributed computing framework.

     => Spark is written in Scala.

     => Spark is polyglot i.e supports multiple languages
 -> Scala, Java, Python & R

     => Spark programs can be executed on mulitple cluster managers
 -> Spark Standalone Scheduler, YARN, Mesos, Kubernites.

   Hadoop EcosSystem (without Spark)
   ---------------------------------
-> Batch Analytics of unstructured data : MapReduce
-> Batch Analytics of Structured data : Hive, Impala, Drill. Pig
-> Streaming Analytics : Storm, Samza, Kafka
-> Predictive Analytics using ML : Mahout
-> Graph Parallel Computations : Giraph


   Spark Unified Framework
   -----------------------
A consistent set of APIs that are processed using same execution engine and built
on common data abstractions to cater to different analytics workloads.

      -> Batch Analytics of unstructured data : Spark Core API (RDD API)
-> Batch Analytics of Structured data : Spark SQL (DataFrames API)
-> Streaming Analytics (Real-time) : Spark Streaming, Structured Streaming
-> Predictive Analytics using ML : Spark MLlib
-> Graph Parallel Computations : Spark GraphX


   Spark Architecture & Building Blocks
   ------------------------------------

1. Cluster Manager (CM)

-> Application is submitted to CM and CM schedules the application.
-> Spark Supports YARN, Mesos, Kubernites & Spark Scheduler
-> CM allocates some executors to the application
-> Each executors is a set of resources like 3 GB RAM & 3 CPU vCores

2. Driver Program
-> Driver programs is created for an application
-> Driver program contains a "SparkContext" object (or "SparkSession" object in case of Spark SQL)
-> Manages the user-code and creates physical execution plan and sends
  tasks to be executed on the executors.

Deploy Modes:

1. client mode : default.
The driver program is launched on the client machine.
The client machine must be on as long as the job is running.

2. cluster mode: The driver run on one of the nodes in the cluster

3. Executors
-> Executors are resource containers allocated by CM to a spark app.
-> Each executor can run multiple parallel tasks (based on # of CPU cores)
-> Each executor process does the same function, but on different partitions of data.

4. SparkContext
-> Created in the driver program.
-> Represents an application and defines the properties of the connection.
-> Sends tasks to the executors.


    RDD (Resilient Distributed Dataset)
    ------------------------------------

=> Fundamental in-memory data abstract of Spark

=> A collection of distributed partitions
-> Each partitition is a collection of objects.

=> RDDs are immutable.

=> RDDs are lazily evaluated.
-> The execution is triggered by action commands ONLY
-> transformations does not cause execution
-> A physical execution plan (job) is created when spark gets an action command.

RDD:
-> data: Partitions
-> meta-data: Lineage DAG (is agraph os dependencies)
     Maintained by the driver.

=> RDD is resilient (to partition unavailability in memory)
-> RDDs can recreate missing partitions on the fly.


   What can we do with an RDD ?
   ----------------------------

1. Transformations
-> Does not trigger execution
-> The output of each transformation is an RDD.
-> Transformations cause RDD lineage DAGs to be created.

2. Actions
-> Produces output
-> triggers execution and causes a physical execution plan to be created.


   How to get started with PySpark
   -------------------------------

Download Spark:  https://spark.apache.org/downloads.html


1. Use PySpark shell.

2. Use any Python IDE such as Spyder, Jupyter Notebook etc.

3. Databricks Community Account
https://community.cloud.databricks.com/login.html


   How to create RDDs
   ------------------
NOTE: rddFile.getNumPartitions()  is used to get number of partitions of an RDD


3 ways to create RDD:

1. Creating RDD from external file.

rdd1 = sc.textFile( <filePath> )
rddFile = sc.textFile( file, 4 )

The default number of partitions is decided by "sc.defaultMinPartitions"


2. Creating RDD from programmatic data (such as collection)

rdd1 = sc.parallelize( range(1, 50) )
rdd1 = sc.parallelize( [1,2,3,4,5,6,7,8,9,10], 2 )

The default number of partitions is decided by "sc.defaultParallelism"


3. By applying transformation on existing RDD

rdd2 = rdd1.map( lambda x: x*2 )

The number of partitions of the output RDD, by default, is equal to the
number of partitions of input RDD. (in general..)

 
  RDD Lineage DAGs
  ----------------
   
    Lineage DAG of an RDD is a graph of all dependencies (parent RDDs heirarchy) all the
    way from the very first RDD.

    rdd1 = sc.parallelize( range(1, 51), 3 )
rdd1 lineage DAG:  (3) rdd1 -> sc.parallelize

    rdd2 = rdd1.map(lambda x: x*2)
rdd2 lineage DAG:  (3) rdd2 -> rdd1.map -> sc.parallelize

    rdd3 = rdd2.filter(lambda x: x > 5)
rdd3 lineage DAG:  (3) rdd3 = rdd2.filter -> rdd1.map -> sc.parallelize

    rdd4 = rdd2.flatMap(lambda x: [x, x*x])
rdd4 lineage DAG:  (3) rdd4 = rdd2.flatMap -> rdd1.map -> sc.parallelize

    rdd4.collect() => [sc.parallelize, map, flatMap] -> rdd4


   RDD Transformation Types
   -------------------------

1. Narrow Transformations
=> There is no shuffling of data
=> Partition to partition transformation
=> Computation of an output partition requires the data from ONLY its
  input partition.
=> map, filter, glom

2. Wide Transformations
=> There is shuffling of data across partitions
=> Output RDD can have different number of partitions than input RDD.
=> Computation as one partition requires data from multiple input partitions
=> distinct


    Executor Memory Structure
    -------------------------

Reference URL: https://spark.apache.org/docs/latest/configuration.html


Let us assume, we request executors with 10 GB RAM.

-> Clusetr Manager allocates exectors with 10.3 GB RAM

1. Reserved Memory  : 300 MB
-> Spark's internal usage.

2. Spark Memory (spark.memory.fraction: 0.6) => 6 GB
-> Used for RDD execution and storage

2.1 Execution Memory
-> Used for execution of RDD tasks and creating RDD partitions.


2.2 Storage Memory (spark.memory.storageFraction = 0.5)  => 3 GB
-> Used for RDD persistence and storing broadcast variables.

            -> Storage memory can not evict execution memory even if execution memory is
               using more than its 3 GB limit. It has to wait until more memory becomes
      available.

   -> Execution memory can evict some partitions from storage, if it requires more
      memory. But, it can evict only that portion that used by storage beyond its
        3 GB limit.


3. User Memory (1 - spark.memory.fraction = 0.4) => 4 GB
-> Used for user code (Python/Scala/Java etc)


   RDD Persistence
   ---------------
is an instaruction to Spark to persist the partitions of the RDD.

rdd1 = sc.textFile( .. )
rdd2 = rdd1.t1(..)
rdd3 = rdd1.t2(..)
rdd4 = rdd3.t3(..)
rdd5 = rdd3.t4(..)
rdd6 = rdd3.t5(..)
rdd7 = rdd5.t6(..)
rdd7.persist( StorageLevel.MEMORY_AND_DISK )        
===> instruction to Spark to persist the partitions of rdd7
rdd8 = rdd7.t7(..)
rdd9 = rdd7.t8(..)

rdd8.collect()
rdd8 => rdd7.t7 => rdd5.t6 => rdd3.t4 => rdd1.t2 => sc.textFile

(sc.textFile, t2, t4, t6, t7) => rdd8 ====> collect

rdd9.collect()
rdd9 => rdd7.t8
(t8)
   

Persistence Commands
--------------------
rdd.persist(pyspark.StorageLevel.DISK_ONLY)
rdd.cache()
rdd.unpersist()

 
        Persistence Storage Levels
        ---------------------------

MEMORY_ONLY (default) : RDD is persisted ONLY in-memory (RAM)

 RDD may be fully persisted, partially persisted or not persisted
 at all based on the available memory.

 Even persisted partitions (in some cases) are prone to eviction.

 Stored in deserialized format


  MEMORY_AND_DISK : RDD is persisted in RAM if available, or persisted on disk.
 
 If partitions are evicted, they are written to disk.

 Stored in deserialized format

DISK_ONLY :

MEMORY_ONLY_SER : Stored in serialized format

MEMORY_AND_DISK_SER :

MEMORY_ONLY_2 : Two copies are stored in different executor nodes
MEMORY_AND_DISK_2


   RDD Execution Flow
   ------------------
=> A job is created and sent to the executors for every action command.

=> The set of transformations that are to be computed for created RDD partitions
  are divided into "stages".

=> Every wide transformation results in a new stage to be created.

  JOB => [sc.textFile, map, filter, sortBy, map, flatMap, distinct, filter, glom, groupBy, map]

  JOB => [Stage1, Stage2, Stage3, Stage 4]

Stage1 => [ sc.textFile, map, filter ]
Stage2 => [ sortBy, map, flatMap ]
Stage3 => [ distinct, filter, glom ]
Stage4 => [ groupBy, map ]

-> Each stage contains a set of tasks that can run in parallel
-> Stages are launched one after another.


   RDD Transformations
   -------------------

    1. map P: U => V
Element to element transformation
input RDD: N elements, output RDD: N elements


    2. filter P: U => Boolean
Only those objects for which the function returns true will be
in the output RDD.
input RDD: N elements, output RDD: <= N elements


    3. glom P: None
Will create an array for each partiton with the elements of that
partiton
input RDD: N elements, output RDD: elements = # of partitions.

rdd1 rdd2 = rdd1.glom()
------                  ------------------
P0: 1,2,1,2,1,3  -> glom -> P0: [1,2,1,2,1,3]
P1: 3,4,5,2,1,5  -> glom -> P1: [3,4,5,2,1,5]
P2: 6,7,6,8,5,2  -> glom -> P2: [6,7,6,8,5,2]

rdd1.count(): 18    rdd2.count(): 3


   4. flatMap P: U => Iterable[V]    (iterable means a collection)
Flattens all the iterables of the function output and gets
a flat collection
input RDD: N elements, output RDD: >= N elements

rdd1.flatMap(lambda x: [x, x+10]).glom().collect()
rddFile.flatMap(lambda x: x.split(" ")).collect()


   5. distinct P: None (optionally takes number partitions as parameter)
Returns an RDD and distinct elements of the inputRDD

rdd2 = rdd1.distinct()
rdd2 = rdd1.distinct(5)


   6. mapPartitions P: Iterator[U] => Iterator[V]
Applies a function on the entire partition (unlike map which
applies the function on each object)

rdd1 rdd2 = rdd1.mapPartitions( lambda x: y )

------                  ------------------
P0: 1,2,1,2,1,3  -> mapPartitions -> P0: ..
P1: 3,4,5,2,1,5  -> mapPartitions -> P1: ..
P2: 6,7,6,8,5,2  -> mapPartitions -> P2: ..

rdd1.mapPartitions(lambda x: [sum(x)] ).collect()


   7. mapPartitionsWithIndex P: (index, Iterator[U]) => Iterator[V]

rdd1.mapPartitionsWithIndex(lambda index, data : [(index, sum(data))] ).collect()


   8. sortBy P: U => V, where V is the value based on which the elements of the
RDD are sorted.

rddWords.sortBy(lambda x: x[-1]).collect()
rddWords.sortBy(lambda x: x[-1], False).collect()  // second param is the ascending sort
rdd1.sortBy(lambda x: x % 5, True, 5).collect()    // third param is the number of output partitions.


    Types of RDDs from usage perspective:

-> Generic RDDs: RDD[U]
-> Pair RDDs:  RDD[(U, V)]


   9. groupBy P: U => V
Returns a pair RDD, where the Key part is the function output, and all
the objects of the input RDD that produced the key will be grouped in
the value.

RDD[U].groupBy(U => V) => RDD[(V, Iterable[U])]

rdd1.groupBy(lambda x: x, 5).mapValues(list).glom().collect()

output = sc.textFile( filePath ) \
  .flatMap(lambda x: x.split(" ")) \
  .groupBy(lambda x: x) \
  .mapValues(lambda x: len(list(x)) )


   10. mapValues P: U => V
Applied only to pair RDDs
The function is applied to the 'value part' ONLY. It won't alter the key
part of the pairs.

rdd2.mapValues(lambda x: x*10).collect()
rddWords.groupBy(lambda x: x).mapValues(lambda x: list(x)).collect()
rddWords.groupBy(lambda x: x).mapValues( list ).collect()


   11. randomSplit P: An array of ratios
Returns an array of RDDs randomly split in the ratios specified.

rddArr = rdd1.randomSplit([0.5, 0.5])
rddArr = rdd1.randomSplit([0.5, 0.5], 345)


   12. repartition P: Number of partitions
Is used to increase / decrese the number of partitions of the output RDD

rdd2 = rdd1.repartition(5)

   13. coalesce P: Number of partitions
Is used to only decrese the number of partitions of the output RDD

rdd2 = rdd1.coalesce(3)

   14. partitionBy P: numberOfPartitions & partition-function (optional)
       Applied ONLY on pair RDDs
Used to determine which elements go to which partition based on the
hash value of the key (or function output of the key if you are using a
       custom partitioning function)

Default Partitioner is "hash Partitioner", hashing is applied to the key
Dafault hashing function is "modulus function"

rdd3 =  rdd2.partitionBy(4)    // default hash-partitioner is applied

def city_partitioner(city):
    return len(city)

rdd3 = rdd2.partitionBy(3, city_partitioner)


    15. union, intersection, subtract & cartesian

P: rdd as a parameter

Let us say we two RDDs: rdd1 with M partitions & rdd2 with N partitions

transformation number of output partitions
-------------------------------------------------------------------
rdd3 = rdd1.union(rdd2) M + N   (narrow)
rdd3 = rdd1.intersection(rdd2) M + N (wide)
rdd3 = rdd1.subtract(rdd2) M + N (wide)
rdd3 = rdd1.cartesian(rdd2) M * N   (wide)


    ...ByKey Transformations
=> Wide transformations
=> Applied only to Pair RDDs.


    16. groupByKey P: None
Group elements based on the key.
Returns a PairRDD where each element has a unique key and grouped
values from the input RDD.

RDD[U, V].groupByKey() => RDD[(U, Iterable[V])]

rddPairs.groupByKey().mapValues(list).collect()

WordCount:

output = sc.textFile( filePath ) \
  .flatMap(lambda x: x.split(" ")) \
  .map(lamda x: (x, 1)) \
  .groupByKey() \
  .mapValues(sum)


     17. reduceByKey P: (U, U) => U
Applies a reduce function to all the values of each unique key and
produces one final value (of same type) for every unique key.

WordCount: (better than groupByKey)

output = sc.textFile( filePath ) \
  .flatMap(lambda x: x.split(" ")) \
  .map(lamda x: (x, 1)) \
  .reduceByKey(lambda x, y: x + y)

     18. sortByKey P: None
Sorts the elements of the pair RDD based on the value of the key

rdd2.sortByKey().glom().collect()
rdd2.sortByKey(False).glom().collect()
rdd2.sortByKey(False, 5).glom().collect()


     19. aggregateByKey Is used when you want reduce the values of each unique-key to
a final values which is different than the type of the value.

Three Parameters:

1. zero-value: Is the initial-value with which all the values of
      each unique-key are merged within each partition.

      Your final value is of the type of zero-value (not of
      the type of elements)

2. Sequence function: Is the function that operates in each partition
(narrow) to merge all the all the values of each unique
key with the zero-value.

3. Combine function: reduces all the values of each unique-key across
partitions using a reduce function.


 use-case: Computing (SUM, COUNT) for each key

 P0:  (a, 10) (b, 10) (c, 20) (a, 30) (c, 40) P0: (a, (180, 5))
 => (a, (40, 2)) (b, (10, 1)) (c, (60, 2))

 P1:  (a, 50) (b, 30) (c, 50) (b, 80) (c, 10) P1: (b, ...)
 => (a, (50, 1)) (b, (110, 2)) (c, (60, 1))

 P2:  (a, 60) (b, 20) (c, 60) (a, 30) (b, 40) P1: (c, ...)
 => (a, (90, 2)) (b, (60, 2)) (c, (60, 1))


  zero-value:  (0, 0)  (int, int)  
  seq-fun:  lambda z, v: (z[0]+v + z[1]+1)
  comb-fun: (lambda a, b: (a[0] + b[0], a[1] + b[1]))



20. JOINS Transformations: join, leftOuterJoin, rightOuterJoin, fullOuterJoin
RDD[(U, V)].join( RDD[U, W] ) => RDD[(U, (V, W))]

join = names1.join(names2)   #inner Join
leftOuterJoin = names1.leftOuterJoin(names2)
rightOuterJoin = names1.rightOuterJoin(names2)
fullOuterJoin = names1.fullOuterJoin(names2)


21. cogroup Is used when you want to join RDDs with duplicate-key
groupByKey => fullOuterJoin.

[('key1', 10), ('key2', 12), ('key1', 7), ('key2', 6), ('key3', 6)]
  => (key1, [10, 7]) (key2, [12, 6]) (key3, [6])

  [('key1', 5), ('key2', 4), ('key2', 7), ('key1', 17), ('key4', 17)]
  => (key1, [5, 17]) (key2, [4,7]) (key4, [17])

  => (key1, ([10, 7], [5, 17])) (key2, ([12, 6], [4,7])) (key3, ([6], [])), (key4, ([], [17]))



      Guidelines to decide on the partitions
     --------------------------------------

=> Don't create too big partitions
-> Your job may fail due to 2 GB limit on shuffle blocks

=> Don't create too few partitions
-> Job will be slow, not using parallelism

=> Don't have too many partitions
-> May result in too much of shuffling of data.

=> ~ 128 MB per partition is good size

=> If the number os partitions is closer to but less than 2000 partitions,
  then bump it up to more than 2000 partitions.
-> Spark used different data structures if number of partitions > 2000

=> You can have arounf 2 to 3 times more partitions than CPU cores.



   RDD Actions
   ------------

1. collect => returns an array will all the data of RDD
2. count
3. countByValue
4. countByKey => Applied to only pair RDD

5. take

6. takeOrdered => rdd1.takeOrdered(10)
  rdd1.takeOrdered(10, lambda x: x%5)

7. takeSample => takeSample(withReplacement = True, 10, [seed])

8. first

9. saveAsTextFile
ex: wordcount.saveAsTextFile("E:\\PySpark\\tmp\\wordcount")

10. foreach

11. reduce
=> reduces an entire RDD into one final value of same type (as elements of the RDD)
  by iterativly applying a reduce function. (U, U) => U
=> Two step process:
step 1: Will reduce every partition into one value
step 2: Will further reduce all partitions t one final value.

rdd1.reduce( (a, b) => a - b )

rdd1
P0: [1, 2, 1, 2, 4, 3, 5]   59
=> -16

P1: [4, 6, 7, 8, 9, 0, 5]
=> -31

P2: [6, 7, 8, 9, 0, 5, 6, 7, 8]
=> -44

   Use-Case
   --------
Find out the average weight of each make of American Cars => (make, average-weight)
Arrange the data in the descending order of average weight.
Save the output as a single text file.


  Shared Variables
  ----------------

      Spark Core API
=> RDD
=>  Shared Variables :  Accumulator, Broadcast Variable


      Function Closure
      ----------------
Represents a serialized copy of all the code that must be visible (to a thread) to perform a
computation. A copy of this closure will will sent every executor process. Hence the variables
that are part of this closure will become local copies of that process and hence can not be
used to implement global counters.


    rdd1 = sc.parallelize(range(1, 6001), 6)

num_primes = 0

def f1(n):
global num_primes
num_primes = num_primes + isPrime(n)

return n*2

def isPrime(n):
return 1 if n is prime-number
else
return 0

rdd2 = rdd1.map(f1)

print( num_primes )


    Accumulators
    ------------
=> Accumulator is a "sharted variable" that can be updated by all the process and
  is maintained by the driver process.

=> Accumulators are used to implement 'global counters'


    rdd1 = sc.parallelize(range(1, 6001), 6)
num_primes = sc.accumulator(0)

def f1(n):
global num_primes
                if (isPrime(n) == 1)  num_primes.add(1)
return n*2

def isPrime(n):
return 1 if n is prime-number
else
return 0

rdd2 = rdd1.map(f1)

print( num_primes.value() )


    Broadcast Variables
    -------------------

=> Driver sends one copy of the broadcast variable to each executor node
  and all the processes within that executor can lookup from that broadcast
  variable.

=> Broadcast variable is not part of function closure
 

dict = {..............}   // 100 MB

bcDictionary = sc.broadcast(dict)

def lookup(n) :
global bcDictionary
return bcDictionary .value[n]

rdd2 = rdd1.map(lookup)
 

    Spark-Submit Command
    --------------------

=> Is a single command that is used to submit any spark application (Python, Scala, java etc)
  to any cluster manager (local, spark standalone, yarn, mesos, k8s)

spark-sumbit --master local
--deploy-mode cluster
--driver-memory 3G
--executor-memory 5G
--driver-cores 3
--num-executors 10
--total-executor-cores 50
wordcount.py <command-line-parameters>

spark-submit --master local E:\PySpark\spark_core\examples\wordcount_cmdargs.py wordcount.txt wordcountoutput


  Spark SQL  (pyspark.sql)
  =========================
   
    => Structured data processing API


    SparkSession
    ------------
=> Represents a user session with its own configuration
=> A single application (sparkContext) can have multiple sparksession object.

spark = SparkSession \
    .builder \
    .appName("Basic Dataframe Operations") \
    .config("spark.master", "local") \
    .getOrCreate()

   DataFrame
   ---------
=> Is a distributed, partitioned, immutable, lazily-evaluated dataset
=> Is a collection of "Row" objects (pyspark.sql.Row)
       -> Row is an object of type "StructType" which is a list of "StructField" objects
-> "StructField" represents a column.

=> DataFrame has two things:
1. data   => collection of Row objects
2. metadata => schema of the DataFrame (structType)

StructType(
  List(StructField(age,LongType,true),
StructField(gender,StringType,true),
StructField(name,StringType,true),
StructField(phone,StringType,true),
StructField(userid,LongType,true)
  )
)


=> We can think of a dataframe like it is RDD[Row] + schema

 
    What type of data can be processed using SparkSQL ?
    ---------------------------------------------------

1. Structured Data File Formats:
-> Parquet (default)
-> ORC
-> JSON
-> CSV (delimited text file format)
2. Hive
3. JDBC -> RDBMS databases, NoSQL databases.


   What are steps in Spark SQL programs
   ------------------------------------

1. Read the data from some structured source or from programmatic data into a dataframe

df1 = spark.read.format("json").load(inputFilePath)


2. We can transform the DataFrames using :

-> DataFrame API transformations.

df2 = df1.select("userid", "name", "age", "phone") \
        .where("age is not null") \
        .groupBy("age").count() \
        .orderBy("count") \
        .limit(4)  

-> Using SQL (spark.sql)

df1.createOrReplaceTempView("users")
spark.catalog.listTables()

qry = """select age, count(*) as count from users
          where age is not null
          group by age
          order by count
          limit 4"""

df3 = spark.sql(qry)

df3.show()
df3.printSchema()

3. Save the contents of the DataFrame into a structured file or database.

df2.write.format("json").save(outputDir)



   Working with different data formats
   ------------------------------------

        JSON
-----
read:
-----
    df1 = spark.read.format("json").load(inputFilePath)
df1 = spark.read.json(inputFilePath)

write:
-----
df2.write.format("json").save(outputDir)
df2.write.mode("overwrite").json(outputDir)

CSV
---
read
-----
df1 = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(inputFilePath)

df1 = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(inputFilePath)

df1 = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sep", "|") \       # optiobnally sepcify the separator
        .csv(inputFilePath)



write
-----
  df2.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .option("sep", "|") \
    .save(outputDir)

Parquet
-------

read
----
df1 = spark.read.format("parquet").load(inputFilePath)
df1 = spark.read.parquet(inputFilePath)

write
-----
df2.write.mode("overwrite").format("parquet").save(outputDir)
df2.write.mode("overwrite").parquet(outputDir)

ORC
---
read
----
df1 = spark.read.format("orc").load(inputFilePath)
df1 = spark.read.orc(inputFilePath)

write
-----
df2.write.mode("overwrite").orc(outputDir)




   DataFrame Transformations
   --------------------------

1. select

df2 = df1.select("userid", "name", "age", "phone")

df2 = df1.select(col("DEST_COUNTRY_NAME").alias("destination"),
                  column("ORIGIN_COUNTRY_NAME").alias("origin"),
                  expr("count"),
                  expr("count + 10 as newCount"),
                  expr("count > 365 as highFrequency"),
                  expr("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME as domestic")
                  )

2. where / filter

df2 = df1.where("phone is not null")
df5 = df4.where(col("highFrequency") == True)

3. orderBy

df2 = df1.orderBy("age", "userid")
df5 = df4.orderBy(col("DEST_COUNTRY_NAME").desc(), col("count").desc())
df5 = df4.orderBy(desc("DEST_COUNTRY_NAME"), asc("count"))


4. groupBy => returns a "GroupedData" object, on which you have apply aggregations

df2 = df1.groupBy("age").count()
df5 = df4.groupBy("domestic").sum("count")
df5 = df4.groupBy("domestic").min("count")

df5 = df4.groupBy("domestic") \
          .agg( count("count").alias("count"),
                sum("count").alias("sum"),
                avg("count").alias("avg"),
                max("count").alias("max"))

5. limit

df2 = df1.limit(4)

6. selectExpr

df2 = df1.selectExpr("DEST_COUNTRY_NAME as destination",
                  "ORIGIN_COUNTRY_NAME as origin",
                  "count",
                  "count + 10 as newCount",
                  "count > 365 as highFrequency",
                  "DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME as domestic" )

7. withColumn

df4 = df1.withColumn("newCount", expr("count + 10")) \
          .withColumn("highFrequency", col("count") > 365) \
          .withColumn("domestic", expr("DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME"))
         
8. withColumnRenamed

df5 = df4.withColumnRenamed("DEST_COUNTRY_NAME", "destination") \
        .withColumnRenamed("ORIGIN_COUNTRY_NAME", "origin")

9. drop
df5 = df4.drop("newCount", "highFrequency")

10. distinct

df4.select("origin", "destination").distinct().count()
df4.select("origin").distinct().count()

11. sample

seed = 25
withReplacement = False
fraction = 0.5

df1.sample(withReplacement, fraction, seed).show()

12. randomSplit

seed = 5
randomDfs = df1.randomSplit([0.25, 0.35, 0.4], 5)

randomDfs[0].count()
randomDfs[1].count()
randomDfs[2].count()

13. repartition & coalesce

df5 = df4.repartition(2)
df5 = df4.repartition( 5, col("origin") )
df5 = df4.repartition( col("origin") )      
=> here the max number of partitionsis based on
  "spark.sql.shuffle.partitions" config option.

=> spark.conf.set("spark.sql.shuffle.partitions", '20')

df6 = df5.coalesce(3)


    LocalTempViews & GlobalTempViews
   ---------------------------------

Local Temp View :
-> Created in SparkSession's local catalog.
df1.createOrReplaceTempView("users")
-> Accessible only from with in that sparksession.

Global Temp View:
-> Created at application scope
df1.createGlobalTempView("gusers")
-> Accessible from any sparksession in that application.


    Applying programmatic schema
    ----------------------------

my_schema  = StructType(
                [StructField('ORIGIN_COUNTRY_NAME', StringType(), True),
                 StructField('DEST_COUNTRY_NAME', StringType(), True),
                 StructField('count', IntegerType(), True)]
            )

df1 = spark.read.schema(my_schema).json(inputFilePath)


    Creating DataFrames using programmatic data
    -------------------------------------------

method 1
        --------

list = [(101, "Raju", 45),
        (102, "Komala", 39),
        (103, "Aditya", 25),
        (104, "Amrita", 23)]

df1 = spark.createDataFrame(list).toDF("id", "name", "age")


method 2
        --------

list = [(101, "Raju", 45), (102, "Komala", 39), (103, "Aditya", 25), (104, "Amrita", 23)]

rdd = spark.sparkContext.parallelize(list)
rddRows = rdd.map(lambda t: Row(t[0], t[1], t[2]) )

mySchema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), False)
        ])

df2 = spark.createDataFrame(rddRows, mySchema)

df2.show()


  Save Modes
  ----------
=> Controls the behaviour when you are writing a DF to an existing directory.

errorifexists  -> default
ignore
append
overwrite

    df2.write \
    .format("csv") \
    .mode("overwrite") \
    .option("header", "true") \
    .save(outputDir)



   Joins
   -----

Supported Joins: inner, left_outer, right_outer, full_outer, left_semi, left_anti

left-semi join:
=> inner join, except that the data comes ONLY from left table.
=> sub-query:
  select * from emp where deptid IN (select distinct id from dept)

left-anti join:
=> you only those records from the left table whose keys are NOT THERE in the
  joined table.
=> sub-query:
  select * from emp where deptid NOT IN (select distinct id from dept)


Join Strategies
----------------

1. Shuffle Joins   (Big Table to Big Table)
-> Shuffle Hash Join
-> Sort Merge Join

2. Broadcast Joins (Big Table to Small Table)
-> automatically performed if one of the tables/DFs is small


Config:  spark.sql.autoBroadcastJoinThreshold = 10 MB


================================================
employee = spark.createDataFrame([
    (1, "Raju", 25, 101),
    (2, "Ramesh", 26, 101),
    (3, "Amrita", 30, 102),
    (4, "Madhu", 32, 102),
    (5, "Aditya", 28, 102),
    (6, "Pranav", 28, 10000)])\
  .toDF("id", "name", "age", "deptid")
 
employee.printSchema()
employee.show()  
 
department = spark.createDataFrame([
    (101, "IT", 1),
    (102, "ITES", 1),
    (103, "Opearation", 1),
    (104, "HRD", 2)])\
  .toDF("id", "deptname", "locationid")
 
department.show()  
department.printSchema()

spark.catalog.listTables()

employee.createOrReplaceTempView("emp")
department.createOrReplaceTempView("dept")

qry = """select *
        from emp left anti join dept
        on emp.deptid = dept.id"""          

output = spark.sql(qry)
output.show()

================================================

joinCol = employee["deptid"] == department["id"]
output = employee.join(department, joinCol, "left_outer")
output.show()

output = employee.join(broadcast(department), joinCol, "inner")

   => The upper limite on the size of the broadcasted DF is 8 GB.



   Working with Hive
   -----------------
     -> Hive is a data warehousing framework in Hadoop
     -> Is a wrapper on top of MapReduce and exposed a SQL like language
(called Hive Query Language).

Hive:          
          -> warehouse location: directory path where Hive stores all its data files.
 -> metastore: is typically an external rdbms (mysql) where Hive store its metadata.


warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Datasorces") \
    .config("spark.master", "local") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()


-----------------------------------
warehouse_location = abspath('spark-warehouse')

spark = SparkSession \
    .builder \
    .appName("Datasorces") \
    .config("spark.master", "local") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()
   
spark.catalog.listTables()    


spark.sql("create database if not exists sparkdemo")
spark.sql("use sparkdemo")

spark.catalog.listTables()

spark.sql("DROP TABLE IF EXISTS movies")
spark.sql("DROP TABLE IF EXISTS ratings")
spark.sql("DROP TABLE IF EXISTS topRatedMovies")
   
createMovies = """CREATE TABLE IF NOT EXISTS
         movies (movieId INT, title STRING, genres STRING)
         ROW FORMAT DELIMITED
         FIELDS TERMINATED BY ','"""
   
loadMovies = """LOAD DATA LOCAL INPATH 'E:/PySpark/data/movielens/moviesNoHeader.csv'
         OVERWRITE INTO TABLE movies"""
   
createRatings = """CREATE TABLE IF NOT EXISTS
         ratings (userId INT, movieId INT, rating DOUBLE, timestamp LONG)
         ROW FORMAT DELIMITED
         FIELDS TERMINATED BY ','"""
   
loadRatings = """LOAD DATA LOCAL INPATH 'E:/PySpark/data/movielens/ratingsNoHeader.csv'
         OVERWRITE INTO TABLE ratings"""
         
spark.sql(createMovies)
spark.sql(loadMovies)
spark.sql(createRatings)
spark.sql(loadRatings)
   
spark.catalog.listTables()
     
#Queries are expressed in HiveQL

moviesDF = spark.sql("SELECT * FROM movies")
ratingsDF = spark.sql("SELECT * FROM ratings")

moviesDF.show()
ratingsDF.show()
           
summaryDf = ratingsDF \
            .groupBy("movieId") \
            .agg(count("rating").alias("ratingCount"), avg("rating").alias("ratingAvg")) \
            .filter("ratingCount > 25") \
            .orderBy(desc("ratingAvg")) \
            .limit(10)
             
summaryDf.show()
   
joinStr = summaryDf["movieId"] == moviesDF["movieId"]
   
summaryDf2 = summaryDf.join(moviesDF, joinStr) \
                .drop(summaryDf["movieId"]) \
                .select("movieId", "title", "ratingCount", "ratingAvg") \
                .orderBy(desc("ratingAvg"))
   
summaryDf2.show()
   
summaryDf2.write.format("hive").saveAsTable("topRatedMovies")
spark.catalog.listTables()
       
topRatedMovies = spark.sql("SELECT * FROM topRatedMovies")
topRatedMovies.show()
   
spark.catalog.listFunctions()  

 
spark.stop()
------------------------------------------------------

    Working with MySQL (RDBMS) - JDBC Format
    ----------------------------------------


 



  Window Operations
  -----------------

    windowSpec = Window \
  .partitionBy("CustomerId", "date") \
  .orderBy("Quantity") \
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)


id dept salary sum(salary)
--------------------------------------------------
101 IT 100000 100000
102 IT 80000 180000
105 IT 40000 220000
109 IT 40000 260000
111 IT 35000 295000

103 Sales 60000 60000
110 Sales 50000 110000
106 Sales 45000 155000

104 HR 65000
107 HR 55000
108 HR 60000


     windowSpec = Window.partitionBy("dept")
.orderBy(desc("salary"))
.rowsBetween(Window.unboundedPreceding, Window.currentRow)
 
sum(salary).over(windowSpec)

windowSpec = Window \
  .partitionBy("CustomerId", "date") \
  .orderBy("Quantity") \
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)
 
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
avgPurchaseQuantity = avg(col("Quantity")).over(windowSpec)
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)
rowNumber = row_number().over(windowSpec)

dfWithDate.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
  .select(
    col("CustomerId"),
    col("date"),
    col("Quantity"),
    rowNumber.alias("rowNumber"),
    purchaseRank.alias("qtyRank"),
    purchaseDenseRank.alias("qtyDenseRank"),
    avgPurchaseQuantity.alias("avgQty"),
    maxPurchaseQuantity.alias("maxQty")).show(500)

  --------------------------------------------------------------------

    Working with Data & Time
    ------------------------

users = spark.createDataFrame([
    (1, "Raju", "05-01-1975"),
    (2, "Ramesh", "07-02-1991"),
    (3, "Amrita", "10-03-1995"),
    (4, "Madhu", "03-04-1997"),
    (5, "Aditya", "02-05-2000"),
    (6, "Pranav", "23-06-1985")])\
  .toDF("id", "name", "dob")
 
users.printSchema()
users.show()  

df2 = users.withColumn("date", to_date(col("dob"), "d-MM-yyyy")) \
        .withColumn("current_timestamp", current_timestamp()) \
        .withColumn("today", current_date()) \
        .withColumn("tomorrow", date_add("current_date", 1)) \
        .withColumn("yesterday", date_sub("current_date", 1))
 
df2.show(10, False)
df2.printSchema()
df2.schema

df3 = users.withColumn("date", to_date(col("dob"), "d-MM-yyyy")) \
        .withColumn("new_date", date_format("date", "dd/MM/yyyy")) \
        .withColumn("year_trunc", date_trunc("year", "date")) \
        .withColumn("month_trunc", date_trunc("month", "date"))

df3.show(10, False)
df3.printSchema()


df4 = users.withColumn("date", to_date(col("dob"), "d-MM-yyyy")) \
        .withColumn("today", current_date()) \
        .withColumn("diff", datediff("today", "date")) \
        .withColumn("month_day", dayofmonth("date")) \
        .withColumn("week_day", dayofweek("date"))
       
df4.show(10, False)
df4.printSchema()        


# Ref link: https://medium.com/expedia-group-tech/deep-dive-into-apache-spark-datetime-functions-b66de737950a

 ----------------------------------------------------------
  Use-case
  --------

  From movies.csv and ratings.csv datasets, fetch the top 20 movies with heighest
  average ratings using DataFrame API.

-> Consider only those movies that are rated by atleast 25 users.
-> Data required: movieId, title, totalRatings, averageRating
-> Show the data in the descending order of averageRating.
-> Save the output as a "pipe-separated-file"

==============================================================================

   Machine Learning & Spark MLlib
   ------------------------------
 
    ML Model : The goal of any ML project.

    "ML Model" is a 'learned entity'
=> learning happens based some "historic data" (or "training data")

=> an algorithm performs an iterative computation on the "training data"
  with a goal to establish a relation between "inputs" and "output" in
  such a way that the "loss" is minimal.

=> The relation berweem the "inputs" and "output" thus established
  is called "a model"

 -----------------------------------------
             model = algorithm( training-data )
          -----------------------------------------

    Terminology
    -----------

     1. Feature : input variables, dimensions, independent variables
     2. Label : Output, dependent variable
     3. Algorithm : Is a iretative mathematical computation that returns a model
     4. Model : Is the learned entity (that has a relation b.w label and features)  
     5. Error : The different between a actual value and predicted value for given data point.
     6. Loss : The cumulative error corresponsign to all point. (R.M.S.E)

 
   Few Popular Algorithms
   ----------------------
1. Linear Regression (straight line)
2. Logistic Regression (log curve)
3. Dicision Tree (is a tree graph)
4. Random Forest (ensemble of decision trees)


   What are the steps in ML
   ------------------------

    1. Data Collection
=> Collection if raw data

    2. Data Prepartion (60 to 65% of time is spent here)
=> Is the process of converting the raw data into a format that can be used
  as training data for an algorithm.

=> output: prepared data  (Feature Vector)

Rules: All the data must be numeric
There should be no null/empty-values.

    3. Training the model
=> The prepared data is fit to an algorithm
=> Algorithm return an ML model

    4. Evaluate the model
=> Split the training data into two sets: 70 % (training) & 30 % (validation)
=> Train the model using "training" dataset
=> Get the preditions using the model for "validation" dataset.  
=> By comparing the predictions with the labels, you can evaluate the model.

    5. Deploy the model.


  Types of Machine Learning
  -------------------------

1. Supervised Learning
Data : Has both "label" and "features" (labeled data)
Training: Create a relation between label and features.
Model: Predicts the label given unseen features.

1.1  Classification
-> The label belongs to few fixed values.
-> Label:  1/0, True/False, Yes/No, [1,2,3,4,5]
-> Ex: Email Spam, Dignostics, Survival Prediction

1.2  Regression
-> The label is a continuous value
-> House Price pridiction

2. Unsupervised Learning
Data: Has only features. no label
Training: Understanding patterns in the data
Model: Groups the data into mulitple categories

2.1   Clustering

2.2   Dimensionality Reduction

 
3. Reinforcement Learning
=> Semi-supervised learning


   Spark MLlib
   -----------

       Popular Libraries:
 => Machine Learning: Spark MLlib, SAS, SciKit Learn, PyTorch
 => Deep Learning: TensorFlow (Google), Keras, PyTorch

       Two libraries:
1. pyspark.mllib   => Based on RDDs (not preferred)
2. pyspak.ml    => Based on DataFrames (current one)


   What do we have in Spark MLlib
   -------------------------------

1. Tools to work with Features:
-> Feature Selector
-> Feature Transformers
-> Feature Extractors

2. Algorithms

3. Pipeline:    
-> Approach to ML in Spark MLlib

4. Model Selection and Tuning
-> TrainValidation Split
-> CrossValidation

5. Utility Packages:  Statistics, LinearAlgebra.


   Basic buliding blocks of MLlib  (pyspark.ml)
   ------------------------------

1. DataFrame => All the data is there in a DataFrame

2. Feature Vector => All features are expressed as one vector column.

-> Dense Vector :   Vectors.dense(0,5,0,0,0,0,0,8,9,0,0,0,12,0)
-> Sparse Vector  :  Vectors.sparse(14, [1,7,8,12], [5,8,9,12])

3. Transformer
=> Takes a DF as input and returns a DF as output
=> All transformers have a method called "transform"
=> Transformer take one (or more) column as input and add a
 (one or more) new trasformed columns in the output DF.

df2 = <transformer>.transform( df1 )

=> Feature Transformers, ML Model  


4. Estimator
=> Takes a DF as input and returns a "Model"
=> Have "fit" method

model = <estimator>.fit( df )

=> Algorithms, RFormula, Pipeline (generally)

5. Pipeline
Set of stages containing Transformers and Estimators.

pl = Pipeline().setStages([T1, T2, T3, E1])
plModel = pl.fit(df)

df => T1 => [df2] => T2 => [df3] => T3 => [df4] -> E1 -> plModel
         
  --------------------------------------------------------------------

   Titanic Survival Prediction
   --------------------------
 
PassengerId,Survived,Pclass,Name,Sex,Age,SibSp,Parch,Ticket,Fare,Cabin,Embarked
1,0,3,"Braund, Mr. Owen Harris",male,22,1,0,A/5 21171,7.25,,S
2,1,1,"Cumings, Mrs. John Bradley (Florence Briggs Thayer)",female,38,1,0,PC 17599,71.2833,C85,C
3,1,3,"Heikkinen, Miss. Laina",female,26,0,0,STON/O2. 3101282,7.925,,S
4,1,1,"Futrelle, Mrs. Jacques Heath (Lily May Peel)",female,35,1,0,113803,53.1,C123,S
5,0,3,"Allen, Mr. William Henry",male,35,0,0,373450,8.05,,S


  label: Survived
  features: Pclass,Sex,Age,SibSp,Parch,Fare,Embarked

 categorical: Sex,Embarked   -> OHE
 numerical: Pclass,Age,SibSp,Parch,Fare

  pipeline stages: genderIndexer, embarkIndexer, genderEncoder, embarkEncoder
                   -> vectorAssembelr -> rf

  pipeline = Pipeline(stages=[genderIndexer, embarkIndexer, genderEncoder,embarkEncoder, assembler, rf])

  traindf.printSchema()
  model = pipeline.fit(traindf)

  ====================================================================

   Spark Streaming
   ----------------

Batch Analytics:
Batch data  : bounded (100 GB)  
Batch processing  : bounded (started @ 10:00 am and ended at 10:15 am)

Streaming Analytics:
Streaming data   : unbounded (unbounded data)  
Stream processing  : real-time processing
    (processing as the data arives)


   Spark Streaming
   ---------------

Two libraries:

-> Spark Streaming  => RDD based
-> Structured Streaming   => Dataframe based

       Spark Streaming : (DStream API)

  => Starting point: streamingContext which is defined with a window.
  => Created micro-batches from streaming data and each micro-batch is represented
  by an RDD
  => This create a continuous flow of RDDs. This is "Descritixed Stream" (DStream)
  => This provides a "near-real-time" processing (i.e provides Seconds scale latencies)


      Types of Streams Nativly Supported by Spark
     
           => Socket Stream
  => File Stream











 







