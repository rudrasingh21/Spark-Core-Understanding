# Spark
Spark Pages

   Home   	   Trees   	   Indices   	   Help   	
Spark 1.1.0 Python API Docs
Package pyspark :: Module rdd :: Class RDD	
[frames] | no frames]
Class RDD
source code

object --+
         |
        RDD
A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable, partitioned collection of elements that can be operated on in parallel.

Instance Methods
 	
__init__(self, jrdd, ctx, jrdd_deserializer)
x.__init__(...) initializes x; see help(type(x)) for signature	source code
 	
id(self)
A unique ID for this RDD (within its SparkContext).	source code
 	
__repr__(self)
repr(x)	source code
 	
context(self)
The SparkContext that this RDD was created on.	source code
 	
cache(self)
Persist this RDD with the default storage level (MEMORY_ONLY_SER).	source code
 	
persist(self, storageLevel)
Set this RDD's storage level to persist its values across operations after the first time it is computed.	source code
 	
unpersist(self)
Mark the RDD as non-persistent, and remove all blocks for it from memory and disk.	source code
 	
checkpoint(self)
Mark this RDD for checkpointing.	source code
 	
isCheckpointed(self)
Return whether this RDD has been checkpointed or not	source code
 	
getCheckpointFile(self)
Gets the name of the file to which this RDD was checkpointed	source code
 	
map(self, f, preservesPartitioning=False)
Return a new RDD by applying a function to each element of this RDD.	source code
 	
flatMap(self, f, preservesPartitioning=False)
Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.	source code
 	
mapPartitions(self, f, preservesPartitioning=False)
Return a new RDD by applying a function to each partition of this RDD.	source code
 	
mapPartitionsWithIndex(self, f, preservesPartitioning=False)
Return a new RDD by applying a function to each partition of this RDD, while tracking the index of the original partition.	source code
 	
mapPartitionsWithSplit(self, f, preservesPartitioning=False)
Deprecated: use mapPartitionsWithIndex instead.	source code
 	
getNumPartitions(self)
Returns the number of partitions in RDD	source code
 	
filter(self, f)
Return a new RDD containing only the elements that satisfy a predicate.	source code
 	
distinct(self)
Return a new RDD containing the distinct elements in this RDD.	source code
 	
sample(self, withReplacement, fraction, seed=None)
Return a sampled subset of this RDD (relies on numpy and falls back on default random generator if numpy is unavailable).	source code
 	
takeSample(self, withReplacement, num, seed=None)
Return a fixed-size sampled subset of this RDD (currently requires numpy).	source code
 	
union(self, other)
Return the union of this RDD and another one.	source code
 	
intersection(self, other)
Return the intersection of this RDD and another one.	source code
 	
__add__(self, other)
Return the union of this RDD and another one.	source code
 	
sortByKey(self, ascending=True, numPartitions=None, keyfunc=lambda x: x)
Sorts this RDD, which is assumed to consist of (key, value) pairs.	source code
 	
sortBy(self, keyfunc, ascending=True, numPartitions=None)
Sorts this RDD by the given keyfunc	source code
 	
glom(self)
Return an RDD created by coalescing all elements within each partition into a list.	source code
 	
cartesian(self, other)
Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of elements (a, b) where a is in self and b is in other.	source code
 	
groupBy(self, f, numPartitions=None)
Return an RDD of grouped items.	source code
 	
pipe(self, command, env={})
Return an RDD created by piping elements to a forked external process.	source code
 	
foreach(self, f)
Applies a function to all elements of this RDD.	source code
 	
foreachPartition(self, f)
Applies a function to each partition of this RDD.	source code
 	
collect(self)
Return a list that contains all of the elements in this RDD.	source code
 	
reduce(self, f)
Reduces the elements of this RDD using the specified commutative and associative binary operator.	source code
 	
fold(self, zeroValue, op)
Aggregate the elements of each partition, and then the results for all the partitions, using a given associative function and a neutral "zero value."	source code
 	
aggregate(self, zeroValue, seqOp, combOp)
Aggregate the elements of each partition, and then the results for all the partitions, using a given combine functions and a neutral "zero value."	source code
 	
max(self)
Find the maximum item in this RDD.	source code
 	
min(self)
Find the minimum item in this RDD.	source code
 	
sum(self)
Add up the elements in this RDD.	source code
 	
count(self)
Return the number of elements in this RDD.	source code
 	
stats(self)
Return a StatCounter object that captures the mean, variance and count of the RDD's elements in one operation.	source code
 	
histogram(self, buckets)
Compute a histogram using the provided buckets.	source code
 	
mean(self)
Compute the mean of this RDD's elements.	source code
 	
variance(self)
Compute the variance of this RDD's elements.	source code
 	
stdev(self)
Compute the standard deviation of this RDD's elements.	source code
 	
sampleStdev(self)
Compute the sample standard deviation of this RDD's elements (which corrects for bias in estimating the standard deviation by dividing by N-1 instead of N).	source code
 	
sampleVariance(self)
Compute the sample variance of this RDD's elements (which corrects for bias in estimating the variance by dividing by N-1 instead of N).	source code
 	
countByValue(self)
Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.	source code
 	
top(self, num)
Get the top N elements from a RDD.	source code
 	
takeOrdered(self, num, key=None)
Get the N elements from a RDD ordered in ascending order or as specified by the optional key function.	source code
 	
take(self, num)
Take the first num elements of the RDD.	source code
 	
first(self)
Return the first element in this RDD.	source code
 	
saveAsNewAPIHadoopDataset(self, conf, keyConverter=None, valueConverter=None)
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the new Hadoop OutputFormat API (mapreduce package).	source code
 	
saveAsNewAPIHadoopFile(self, path, outputFormatClass, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, conf=None)
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the new Hadoop OutputFormat API (mapreduce package).	source code
 	
saveAsHadoopDataset(self, conf, keyConverter=None, valueConverter=None)
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the old Hadoop OutputFormat API (mapred package).	source code
 	
saveAsHadoopFile(self, path, outputFormatClass, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, conf=None, compressionCodecClass=None)
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the old Hadoop OutputFormat API (mapred package).	source code
 	
saveAsSequenceFile(self, path, compressionCodecClass=None)
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the org.apache.hadoop.io.Writable types that we convert from the RDD's key and value types.	source code
 	
saveAsPickleFile(self, path, batchSize=10)
Save this RDD as a SequenceFile of serialized objects.	source code
 	
saveAsTextFile(self, path)
Save this RDD as a text file, using string representations of elements.	source code
 	
collectAsMap(self)
Return the key-value pairs in this RDD to the master as a dictionary.	source code
 	
keys(self)
Return an RDD with the keys of each tuple.	source code
 	
values(self)
Return an RDD with the values of each tuple.	source code
 	
reduceByKey(self, func, numPartitions=None)
Merge the values for each key using an associative reduce function.	source code
 	
reduceByKeyLocally(self, func)
Merge the values for each key using an associative reduce function, but return the results immediately to the master as a dictionary.	source code
 	
countByKey(self)
Count the number of elements for each key, and return the result to the master as a dictionary.	source code
 	
join(self, other, numPartitions=None)
Return an RDD containing all pairs of elements with matching keys in self and other.	source code
 	
leftOuterJoin(self, other, numPartitions=None)
Perform a left outer join of self and other.	source code
 	
rightOuterJoin(self, other, numPartitions=None)
Perform a right outer join of self and other.	source code
 	
partitionBy(self, numPartitions, partitionFunc=portable_hash)
Return a copy of the RDD partitioned using the specified partitioner.	source code
 	
combineByKey(self, createCombiner, mergeValue, mergeCombiners, numPartitions=None)
Generic function to combine the elements for each key using a custom set of aggregation functions.	source code
 	
aggregateByKey(self, zeroValue, seqFunc, combFunc, numPartitions=None)
Aggregate the values of each key, using given combine functions and a neutral "zero value".	source code
 	
foldByKey(self, zeroValue, func, numPartitions=None)
Merge the values for each key using an associative function "func" and a neutral "zeroValue" which may be added to the result an arbitrary number of times, and must not change the result (e.g., 0 for addition, or 1 for multiplication.).	source code
 	
groupByKey(self, numPartitions=None)
Group the values for each key in the RDD into a single sequence.	source code
 	
flatMapValues(self, f)
Pass each value in the key-value pair RDD through a flatMap function without changing the keys; this also retains the original RDD's partitioning.	source code
 	
mapValues(self, f)
Pass each value in the key-value pair RDD through a map function without changing the keys; this also retains the original RDD's partitioning.	source code
 	
groupWith(self, other, *others)
Alias for cogroup but with support for multiple RDDs.	source code
 	
cogroup(self, other, numPartitions=None)
For each key k in self or other, return a resulting RDD that contains a tuple with the list of values for that key in self as well as other.	source code
 	
sampleByKey(self, withReplacement, fractions, seed=None)
Return a subset of this RDD sampled by key (via stratified sampling).	source code
 	
subtractByKey(self, other, numPartitions=None)
Return each (key, value) pair in self that has no pair with matching key in other.	source code
 	
subtract(self, other, numPartitions=None)
Return each value in self that is not contained in other.	source code
 	
keyBy(self, f)
Creates tuples of the elements in this RDD by applying f.	source code
 	
repartition(self, numPartitions)
Return a new RDD that has exactly numPartitions partitions.	source code
 	
coalesce(self, numPartitions, shuffle=False)
Return a new RDD that is reduced into `numPartitions` partitions.	source code
 	
zip(self, other)
Zips this RDD with another one, returning key-value pairs with the first element in each RDD second element in each RDD, etc.	source code
 	
zipWithIndex(self)
Zips this RDD with its element indices.	source code
 	
zipWithUniqueId(self)
Zips this RDD with generated unique Long ids.	source code
 	
name(self)
Return the name of this RDD.	source code
 	
setName(self, name)
Assign a name to this RDD.	source code
 	
toDebugString(self)
A description of this RDD and its recursive dependencies for debugging.	source code
 	
getStorageLevel(self)
Get the RDD's current storage level.	source code
Inherited from object: __delattr__, __format__, __getattribute__, __hash__, __new__, __reduce__, __reduce_ex__, __setattr__, __sizeof__, __str__, __subclasshook__

Properties
Inherited from object: __class__

Method Details
__init__(self, jrdd, ctx, jrdd_deserializer) 
(Constructor)
source code 
x.__init__(...) initializes x; see help(type(x)) for signature

Overrides: object.__init__
(inherited documentation)
__repr__(self) 
(Representation operator)
source code 
repr(x)

Overrides: object.__repr__
(inherited documentation)
context(self)
source code 
The SparkContext that this RDD was created on.

Decorators:
@property
persist(self, storageLevel)
source code 
Set this RDD's storage level to persist its values across operations after the first time it is computed. This can only be used to assign a new storage level if the RDD does not have a storage level set yet.

checkpoint(self)
source code 
Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint directory set with SparkContext.setCheckpointDir() and all references to its parent RDDs will be removed. This function must be called before any job has been executed on this RDD. It is strongly recommended that this RDD is persisted in memory, otherwise saving it on a file will require recomputation.

map(self, f, preservesPartitioning=False)
source code 
Return a new RDD by applying a function to each element of this RDD.

>>> rdd = sc.parallelize(["b", "a", "c"])
>>> sorted(rdd.map(lambda x: (x, 1)).collect())
[('a', 1), ('b', 1), ('c', 1)]
flatMap(self, f, preservesPartitioning=False)
source code 
Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results.

>>> rdd = sc.parallelize([2, 3, 4])
>>> sorted(rdd.flatMap(lambda x: range(1, x)).collect())
[1, 1, 1, 2, 2, 3]
>>> sorted(rdd.flatMap(lambda x: [(x, x), (x, x)]).collect())
[(2, 2), (2, 2), (3, 3), (3, 3), (4, 4), (4, 4)]
mapPartitions(self, f, preservesPartitioning=False)
source code 
Return a new RDD by applying a function to each partition of this RDD.

>>> rdd = sc.parallelize([1, 2, 3, 4], 2)
>>> def f(iterator): yield sum(iterator)
>>> rdd.mapPartitions(f).collect()
[3, 7]
mapPartitionsWithIndex(self, f, preservesPartitioning=False)
source code 
Return a new RDD by applying a function to each partition of this RDD, while tracking the index of the original partition.

>>> rdd = sc.parallelize([1, 2, 3, 4], 4)
>>> def f(splitIndex, iterator): yield splitIndex
>>> rdd.mapPartitionsWithIndex(f).sum()
6
mapPartitionsWithSplit(self, f, preservesPartitioning=False)
source code 
Deprecated: use mapPartitionsWithIndex instead.

Return a new RDD by applying a function to each partition of this RDD, while tracking the index of the original partition.

>>> rdd = sc.parallelize([1, 2, 3, 4], 4)
>>> def f(splitIndex, iterator): yield splitIndex
>>> rdd.mapPartitionsWithSplit(f).sum()
6
getNumPartitions(self)
source code 
Returns the number of partitions in RDD

>>> rdd = sc.parallelize([1, 2, 3, 4], 2)
>>> rdd.getNumPartitions()
2
filter(self, f)
source code 
Return a new RDD containing only the elements that satisfy a predicate.

>>> rdd = sc.parallelize([1, 2, 3, 4, 5])
>>> rdd.filter(lambda x: x % 2 == 0).collect()
[2, 4]
distinct(self)
source code 
Return a new RDD containing the distinct elements in this RDD.

>>> sorted(sc.parallelize([1, 1, 2, 3]).distinct().collect())
[1, 2, 3]
takeSample(self, withReplacement, num, seed=None)
source code 
Return a fixed-size sampled subset of this RDD (currently requires numpy).

>>> rdd = sc.parallelize(range(0, 10))
>>> len(rdd.takeSample(True, 20, 1))
20
>>> len(rdd.takeSample(False, 5, 2))
5
>>> len(rdd.takeSample(False, 15, 3))
10
union(self, other)
source code 
Return the union of this RDD and another one.

>>> rdd = sc.parallelize([1, 1, 2, 3])
>>> rdd.union(rdd).collect()
[1, 1, 2, 3, 1, 1, 2, 3]
intersection(self, other)
source code 
Return the intersection of this RDD and another one. The output will not contain any duplicate elements, even if the input RDDs did.

Note that this method performs a shuffle internally.

>>> rdd1 = sc.parallelize([1, 10, 2, 3, 4, 5])
>>> rdd2 = sc.parallelize([1, 6, 2, 3, 7, 8])
>>> rdd1.intersection(rdd2).collect()
[1, 2, 3]
__add__(self, other) 
(Addition operator)
source code 
Return the union of this RDD and another one.

>>> rdd = sc.parallelize([1, 1, 2, 3])
>>> (rdd + rdd).collect()
[1, 1, 2, 3, 1, 1, 2, 3]
sortByKey(self, ascending=True, numPartitions=None, keyfunc=lambda x: x)
source code 
Sorts this RDD, which is assumed to consist of (key, value) pairs. # noqa

>>> tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
>>> sc.parallelize(tmp).sortByKey().first()
('1', 3)
>>> sc.parallelize(tmp).sortByKey(True, 1).collect()
[('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
>>> sc.parallelize(tmp).sortByKey(True, 2).collect()
[('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
>>> tmp2 = [('Mary', 1), ('had', 2), ('a', 3), ('little', 4), ('lamb', 5)]
>>> tmp2.extend([('whose', 6), ('fleece', 7), ('was', 8), ('white', 9)])
>>> sc.parallelize(tmp2).sortByKey(True, 3, keyfunc=lambda k: k.lower()).collect()
[('a', 3), ('fleece', 7), ('had', 2), ('lamb', 5),...('white', 9), ('whose', 6)]
sortBy(self, keyfunc, ascending=True, numPartitions=None)
source code 
Sorts this RDD by the given keyfunc

>>> tmp = [('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
>>> sc.parallelize(tmp).sortBy(lambda x: x[0]).collect()
[('1', 3), ('2', 5), ('a', 1), ('b', 2), ('d', 4)]
>>> sc.parallelize(tmp).sortBy(lambda x: x[1]).collect()
[('a', 1), ('b', 2), ('1', 3), ('d', 4), ('2', 5)]
glom(self)
source code 
Return an RDD created by coalescing all elements within each partition into a list.

>>> rdd = sc.parallelize([1, 2, 3, 4], 2)
>>> sorted(rdd.glom().collect())
[[1, 2], [3, 4]]
cartesian(self, other)
source code 
Return the Cartesian product of this RDD and another one, that is, the RDD of all pairs of elements (a, b) where a is in self and b is in other.

>>> rdd = sc.parallelize([1, 2])
>>> sorted(rdd.cartesian(rdd).collect())
[(1, 1), (1, 2), (2, 1), (2, 2)]
groupBy(self, f, numPartitions=None)
source code 
Return an RDD of grouped items.

>>> rdd = sc.parallelize([1, 1, 2, 3, 5, 8])
>>> result = rdd.groupBy(lambda x: x % 2).collect()
>>> sorted([(x, sorted(y)) for (x, y) in result])
[(0, [2, 8]), (1, [1, 1, 3, 5])]
pipe(self, command, env={})
source code 
Return an RDD created by piping elements to a forked external process.

>>> sc.parallelize(['1', '2', '', '3']).pipe('cat').collect()
['1', '2', '', '3']
foreach(self, f)
source code 
Applies a function to all elements of this RDD.

>>> def f(x): print x
>>> sc.parallelize([1, 2, 3, 4, 5]).foreach(f)
foreachPartition(self, f)
source code 
Applies a function to each partition of this RDD.

>>> def f(iterator):
...      for x in iterator:
...           print x
...      yield None
>>> sc.parallelize([1, 2, 3, 4, 5]).foreachPartition(f)
reduce(self, f)
source code 
Reduces the elements of this RDD using the specified commutative and associative binary operator. Currently reduces partitions locally.

>>> from operator import add
>>> sc.parallelize([1, 2, 3, 4, 5]).reduce(add)
15
>>> sc.parallelize((2 for _ in range(10))).map(lambda x: 1).cache().reduce(add)
10
fold(self, zeroValue, op)
source code 
Aggregate the elements of each partition, and then the results for all the partitions, using a given associative function and a neutral "zero value."

The function op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object allocation; however, it should not modify t2.

>>> from operator import add
>>> sc.parallelize([1, 2, 3, 4, 5]).fold(0, add)
15
aggregate(self, zeroValue, seqOp, combOp)
source code 
Aggregate the elements of each partition, and then the results for all the partitions, using a given combine functions and a neutral "zero value."

The functions op(t1, t2) is allowed to modify t1 and return it as its result value to avoid object allocation; however, it should not modify t2.

The first function (seqOp) can return a different result type, U, than the type of this RDD. Thus, we need one operation for merging a T into an U and one operation for merging two U

>>> seqOp = (lambda x, y: (x[0] + y, x[1] + 1))
>>> combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
>>> sc.parallelize([1, 2, 3, 4]).aggregate((0, 0), seqOp, combOp)
(10, 4)
>>> sc.parallelize([]).aggregate((0, 0), seqOp, combOp)
(0, 0)
max(self)
source code 
Find the maximum item in this RDD.

>>> sc.parallelize([1.0, 5.0, 43.0, 10.0]).max()
43.0
min(self)
source code 
Find the minimum item in this RDD.

>>> sc.parallelize([1.0, 5.0, 43.0, 10.0]).min()
1.0
sum(self)
source code 
Add up the elements in this RDD.

>>> sc.parallelize([1.0, 2.0, 3.0]).sum()
6.0
count(self)
source code 
Return the number of elements in this RDD.

>>> sc.parallelize([2, 3, 4]).count()
3
histogram(self, buckets)
source code 
Compute a histogram using the provided buckets. The buckets are all open to the right except for the last which is closed. e.g. [1,10,20,50] means the buckets are [1,10) [10,20) [20,50], which means 1<=x<10, 10<=x<20, 20<=x<=50. And on the input of 1 and 50 we would have a histogram of 1,0,1.

If your histogram is evenly spaced (e.g. [0, 10, 20, 30]), this can be switched from an O(log n) inseration to O(1) per element(where n = # buckets).

Buckets must be sorted and not contain any duplicates, must be at least two elements.

If `buckets` is a number, it will generates buckets which are evenly spaced between the minimum and maximum of the RDD. For example, if the min value is 0 and the max is 100, given buckets as 2, the resulting buckets will be [0,50) [50,100]. buckets must be at least 1 If the RDD contains infinity, NaN throws an exception If the elements in RDD do not vary (max == min) always returns a single bucket.

It will return an tuple of buckets and histogram.

>>> rdd = sc.parallelize(range(51))
>>> rdd.histogram(2)
([0, 25, 50], [25, 26])
>>> rdd.histogram([0, 5, 25, 50])
([0, 5, 25, 50], [5, 20, 26])
>>> rdd.histogram([0, 15, 30, 45, 60])  # evenly spaced buckets
([0, 15, 30, 45, 60], [15, 15, 15, 6])
>>> rdd = sc.parallelize(["ab", "ac", "b", "bd", "ef"])
>>> rdd.histogram(("a", "b", "c"))
(('a', 'b', 'c'), [2, 2])
mean(self)
source code 
Compute the mean of this RDD's elements.

>>> sc.parallelize([1, 2, 3]).mean()
2.0
variance(self)
source code 
Compute the variance of this RDD's elements.

>>> sc.parallelize([1, 2, 3]).variance()
0.666...
stdev(self)
source code 
Compute the standard deviation of this RDD's elements.

>>> sc.parallelize([1, 2, 3]).stdev()
0.816...
sampleStdev(self)
source code 
Compute the sample standard deviation of this RDD's elements (which corrects for bias in estimating the standard deviation by dividing by N-1 instead of N).

>>> sc.parallelize([1, 2, 3]).sampleStdev()
1.0
sampleVariance(self)
source code 
Compute the sample variance of this RDD's elements (which corrects for bias in estimating the variance by dividing by N-1 instead of N).

>>> sc.parallelize([1, 2, 3]).sampleVariance()
1.0
countByValue(self)
source code 
Return the count of each unique value in this RDD as a dictionary of (value, count) pairs.

>>> sorted(sc.parallelize([1, 2, 1, 2, 2], 2).countByValue().items())
[(1, 2), (2, 3)]
top(self, num)
source code 
Get the top N elements from a RDD.

Note: It returns the list sorted in descending order. >>> sc.parallelize([10, 4, 2, 12, 3]).top(1) [12] >>> sc.parallelize([2, 3, 4, 5, 6], 2).top(2) [6, 5]

takeOrdered(self, num, key=None)
source code 
Get the N elements from a RDD ordered in ascending order or as specified by the optional key function.

>>> sc.parallelize([10, 1, 2, 9, 3, 4, 5, 6, 7]).takeOrdered(6)
[1, 2, 3, 4, 5, 6]
>>> sc.parallelize([10, 1, 2, 9, 3, 4, 5, 6, 7], 2).takeOrdered(6, key=lambda x: -x)
[10, 9, 7, 6, 5, 4]
take(self, num)
source code 
Take the first num elements of the RDD.

It works by first scanning one partition, and use the results from that partition to estimate the number of additional partitions needed to satisfy the limit.

Translated from the Scala implementation in RDD#take().

>>> sc.parallelize([2, 3, 4, 5, 6]).cache().take(2)
[2, 3]
>>> sc.parallelize([2, 3, 4, 5, 6]).take(10)
[2, 3, 4, 5, 6]
>>> sc.parallelize(range(100), 100).filter(lambda x: x > 90).take(3)
[91, 92, 93]
first(self)
source code 
Return the first element in this RDD.

>>> sc.parallelize([2, 3, 4]).first()
2
saveAsNewAPIHadoopDataset(self, conf, keyConverter=None, valueConverter=None)
source code 
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the new Hadoop OutputFormat API (mapreduce package). Keys/values are converted for output using either user specified converters or, by default, org.apache.spark.api.python.JavaToWritableConverter.

Parameters:
conf - Hadoop job configuration, passed in as a dict
keyConverter - (None by default)
valueConverter - (None by default)
saveAsNewAPIHadoopFile(self, path, outputFormatClass, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, conf=None)
source code 
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the new Hadoop OutputFormat API (mapreduce package). Key and value types will be inferred if not specified. Keys and values are converted for output using either user specified converters or org.apache.spark.api.python.JavaToWritableConverter. The conf is applied on top of the base Hadoop conf associated with the SparkContext of this RDD to create a merged Hadoop MapReduce job configuration for saving the data.

Parameters:
path - path to Hadoop file
outputFormatClass - fully qualified classname of Hadoop OutputFormat (e.g. "org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat")
keyClass - fully qualified classname of key Writable class (e.g. "org.apache.hadoop.io.IntWritable", None by default)
valueClass - fully qualified classname of value Writable class (e.g. "org.apache.hadoop.io.Text", None by default)
keyConverter - (None by default)
valueConverter - (None by default)
conf - Hadoop job configuration, passed in as a dict (None by default)
saveAsHadoopDataset(self, conf, keyConverter=None, valueConverter=None)
source code 
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the old Hadoop OutputFormat API (mapred package). Keys/values are converted for output using either user specified converters or, by default, org.apache.spark.api.python.JavaToWritableConverter.

Parameters:
conf - Hadoop job configuration, passed in as a dict
keyConverter - (None by default)
valueConverter - (None by default)
saveAsHadoopFile(self, path, outputFormatClass, keyClass=None, valueClass=None, keyConverter=None, valueConverter=None, conf=None, compressionCodecClass=None)
source code 
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the old Hadoop OutputFormat API (mapred package). Key and value types will be inferred if not specified. Keys and values are converted for output using either user specified converters or org.apache.spark.api.python.JavaToWritableConverter. The conf is applied on top of the base Hadoop conf associated with the SparkContext of this RDD to create a merged Hadoop MapReduce job configuration for saving the data.

Parameters:
path - path to Hadoop file
outputFormatClass - fully qualified classname of Hadoop OutputFormat (e.g. "org.apache.hadoop.mapred.SequenceFileOutputFormat")
keyClass - fully qualified classname of key Writable class (e.g. "org.apache.hadoop.io.IntWritable", None by default)
valueClass - fully qualified classname of value Writable class (e.g. "org.apache.hadoop.io.Text", None by default)
keyConverter - (None by default)
valueConverter - (None by default)
conf - (None by default)
compressionCodecClass - (None by default)
saveAsSequenceFile(self, path, compressionCodecClass=None)
source code 
Output a Python RDD of key-value pairs (of form RDD[(K, V)]) to any Hadoop file system, using the org.apache.hadoop.io.Writable types that we convert from the RDD's key and value types. The mechanism is as follows:

Pyrolite is used to convert pickled Python RDD into RDD of Java objects.
Keys and values of this Java RDD are converted to Writables and written out.
Parameters:
path - path to sequence file
compressionCodecClass - (None by default)
saveAsPickleFile(self, path, batchSize=10)
source code 
Save this RDD as a SequenceFile of serialized objects. The serializer used is pyspark.serializers.PickleSerializer, default batch size is 10.

>>> tmpFile = NamedTemporaryFile(delete=True)
>>> tmpFile.close()
>>> sc.parallelize([1, 2, 'spark', 'rdd']).saveAsPickleFile(tmpFile.name, 3)
>>> sorted(sc.pickleFile(tmpFile.name, 5).collect())
[1, 2, 'rdd', 'spark']
saveAsTextFile(self, path)
source code 
Save this RDD as a text file, using string representations of elements.

>>> tempFile = NamedTemporaryFile(delete=True)
>>> tempFile.close()
>>> sc.parallelize(range(10)).saveAsTextFile(tempFile.name)
>>> from fileinput import input
>>> from glob import glob
>>> ''.join(sorted(input(glob(tempFile.name + "/part-0000*"))))
'0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n'
Empty lines are tolerated when saving to text files.

>>> tempFile2 = NamedTemporaryFile(delete=True)
>>> tempFile2.close()
>>> sc.parallelize(['', 'foo', '', 'bar', '']).saveAsTextFile(tempFile2.name)
>>> ''.join(sorted(input(glob(tempFile2.name + "/part-0000*"))))
'\n\n\nbar\nfoo\n'
collectAsMap(self)
source code 
Return the key-value pairs in this RDD to the master as a dictionary.

>>> m = sc.parallelize([(1, 2), (3, 4)]).collectAsMap()
>>> m[1]
2
>>> m[3]
4
keys(self)
source code 
Return an RDD with the keys of each tuple.

>>> m = sc.parallelize([(1, 2), (3, 4)]).keys()
>>> m.collect()
[1, 3]
values(self)
source code 
Return an RDD with the values of each tuple.

>>> m = sc.parallelize([(1, 2), (3, 4)]).values()
>>> m.collect()
[2, 4]
reduceByKey(self, func, numPartitions=None)
source code 
Merge the values for each key using an associative reduce function.

This will also perform the merging locally on each mapper before sending results to a reducer, similarly to a "combiner" in MapReduce.

Output will be hash-partitioned with numPartitions partitions, or the default parallelism level if numPartitions is not specified.

>>> from operator import add
>>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
>>> sorted(rdd.reduceByKey(add).collect())
[('a', 2), ('b', 1)]
reduceByKeyLocally(self, func)
source code 
Merge the values for each key using an associative reduce function, but return the results immediately to the master as a dictionary.

This will also perform the merging locally on each mapper before sending results to a reducer, similarly to a "combiner" in MapReduce.

>>> from operator import add
>>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
>>> sorted(rdd.reduceByKeyLocally(add).items())
[('a', 2), ('b', 1)]
countByKey(self)
source code 
Count the number of elements for each key, and return the result to the master as a dictionary.

>>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
>>> sorted(rdd.countByKey().items())
[('a', 2), ('b', 1)]
join(self, other, numPartitions=None)
source code 
Return an RDD containing all pairs of elements with matching keys in self and other.

Each pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in self and (k, v2) is in other.

Performs a hash join across the cluster.

>>> x = sc.parallelize([("a", 1), ("b", 4)])
>>> y = sc.parallelize([("a", 2), ("a", 3)])
>>> sorted(x.join(y).collect())
[('a', (1, 2)), ('a', (1, 3))]
leftOuterJoin(self, other, numPartitions=None)
source code 
Perform a left outer join of self and other.

For each element (k, v) in self, the resulting RDD will either contain all pairs (k, (v, w)) for w in other, or the pair (k, (v, None)) if no elements in other have key k.

Hash-partitions the resulting RDD into the given number of partitions.

>>> x = sc.parallelize([("a", 1), ("b", 4)])
>>> y = sc.parallelize([("a", 2)])
>>> sorted(x.leftOuterJoin(y).collect())
[('a', (1, 2)), ('b', (4, None))]
rightOuterJoin(self, other, numPartitions=None)
source code 
Perform a right outer join of self and other.

For each element (k, w) in other, the resulting RDD will either contain all pairs (k, (v, w)) for v in this, or the pair (k, (None, w)) if no elements in self have key k.

Hash-partitions the resulting RDD into the given number of partitions.

>>> x = sc.parallelize([("a", 1), ("b", 4)])
>>> y = sc.parallelize([("a", 2)])
>>> sorted(y.rightOuterJoin(x).collect())
[('a', (2, 1)), ('b', (None, 4))]
partitionBy(self, numPartitions, partitionFunc=portable_hash)
source code 
Return a copy of the RDD partitioned using the specified partitioner.

>>> pairs = sc.parallelize([1, 2, 3, 4, 2, 4, 1]).map(lambda x: (x, x))
>>> sets = pairs.partitionBy(2).glom().collect()
>>> set(sets[0]).intersection(set(sets[1]))
set([])
combineByKey(self, createCombiner, mergeValue, mergeCombiners, numPartitions=None)
source code 
Generic function to combine the elements for each key using a custom set of aggregation functions.

Turns an RDD[(K, V)] into a result of type RDD[(K, C)], for a "combined type" C. Note that V and C can be different -- for example, one might group an RDD of type (Int, Int) into an RDD of type (Int, List[Int]).

Users provide three functions:

createCombiner, which turns a V into a C (e.g., creates a one-element list)
mergeValue, to merge a V into a C (e.g., adds it to the end of a list)
mergeCombiners, to combine two C's into a single one.
In addition, users can control the partitioning of the output RDD.

>>> x = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
>>> def f(x): return x
>>> def add(a, b): return a + str(b)
>>> sorted(x.combineByKey(str, add, add).collect())
[('a', '11'), ('b', '1')]
aggregateByKey(self, zeroValue, seqFunc, combFunc, numPartitions=None)
source code 
Aggregate the values of each key, using given combine functions and a neutral "zero value". This function can return a different result type, U, than the type of the values in this RDD, V. Thus, we need one operation for merging a V into a U and one operation for merging two U's, The former operation is used for merging values within a partition, and the latter is used for merging values between partitions. To avoid memory allocation, both of these functions are allowed to modify and return their first argument instead of creating a new U.

foldByKey(self, zeroValue, func, numPartitions=None)
source code 
Merge the values for each key using an associative function "func" and a neutral "zeroValue" which may be added to the result an arbitrary number of times, and must not change the result (e.g., 0 for addition, or 1 for multiplication.).

>>> rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
>>> from operator import add
>>> rdd.foldByKey(0, add).collect()
[('a', 2), ('b', 1)]
groupByKey(self, numPartitions=None)
source code 
Group the values for each key in the RDD into a single sequence. Hash-partitions the resulting RDD with into numPartitions partitions.

Note: If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using reduceByKey will provide much better performance.

>>> x = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
>>> map((lambda (x,y): (x, list(y))), sorted(x.groupByKey().collect()))
[('a', [1, 1]), ('b', [1])]
flatMapValues(self, f)
source code 
Pass each value in the key-value pair RDD through a flatMap function without changing the keys; this also retains the original RDD's partitioning.

>>> x = sc.parallelize([("a", ["x", "y", "z"]), ("b", ["p", "r"])])
>>> def f(x): return x
>>> x.flatMapValues(f).collect()
[('a', 'x'), ('a', 'y'), ('a', 'z'), ('b', 'p'), ('b', 'r')]
mapValues(self, f)
source code 
Pass each value in the key-value pair RDD through a map function without changing the keys; this also retains the original RDD's partitioning.

>>> x = sc.parallelize([("a", ["apple", "banana", "lemon"]), ("b", ["grapes"])])
>>> def f(x): return len(x)
>>> x.mapValues(f).collect()
[('a', 3), ('b', 1)]
groupWith(self, other, *others)
source code 
Alias for cogroup but with support for multiple RDDs.

>>> w = sc.parallelize([("a", 5), ("b", 6)])
>>> x = sc.parallelize([("a", 1), ("b", 4)])
>>> y = sc.parallelize([("a", 2)])
>>> z = sc.parallelize([("b", 42)])
>>> map((lambda (x,y): (x, (list(y[0]), list(y[1]), list(y[2]), list(y[3])))),                 sorted(list(w.groupWith(x, y, z).collect())))
[('a', ([5], [1], [2], [])), ('b', ([6], [4], [], [42]))]
cogroup(self, other, numPartitions=None)
source code 
For each key k in self or other, return a resulting RDD that contains a tuple with the list of values for that key in self as well as other.

>>> x = sc.parallelize([("a", 1), ("b", 4)])
>>> y = sc.parallelize([("a", 2)])
>>> map((lambda (x,y): (x, (list(y[0]), list(y[1])))), sorted(list(x.cogroup(y).collect())))
[('a', ([1], [2])), ('b', ([4], []))]
sampleByKey(self, withReplacement, fractions, seed=None)
source code 
Return a subset of this RDD sampled by key (via stratified sampling). Create a sample of this RDD using variable sampling rates for different keys as specified by fractions, a key to sampling rate map.

>>> fractions = {"a": 0.2, "b": 0.1}
>>> rdd = sc.parallelize(fractions.keys()).cartesian(sc.parallelize(range(0, 1000)))
>>> sample = dict(rdd.sampleByKey(False, fractions, 2).groupByKey().collect())
>>> 100 < len(sample["a"]) < 300 and 50 < len(sample["b"]) < 150
True
>>> max(sample["a"]) <= 999 and min(sample["a"]) >= 0
True
>>> max(sample["b"]) <= 999 and min(sample["b"]) >= 0
True
subtractByKey(self, other, numPartitions=None)
source code 
Return each (key, value) pair in self that has no pair with matching key in other.

>>> x = sc.parallelize([("a", 1), ("b", 4), ("b", 5), ("a", 2)])
>>> y = sc.parallelize([("a", 3), ("c", None)])
>>> sorted(x.subtractByKey(y).collect())
[('b', 4), ('b', 5)]
subtract(self, other, numPartitions=None)
source code 
Return each value in self that is not contained in other.

>>> x = sc.parallelize([("a", 1), ("b", 4), ("b", 5), ("a", 3)])
>>> y = sc.parallelize([("a", 3), ("c", None)])
>>> sorted(x.subtract(y).collect())
[('a', 1), ('b', 4), ('b', 5)]
keyBy(self, f)
source code 
Creates tuples of the elements in this RDD by applying f.

>>> x = sc.parallelize(range(0,3)).keyBy(lambda x: x*x)
>>> y = sc.parallelize(zip(range(0,5), range(0,5)))
>>> map((lambda (x,y): (x, (list(y[0]), (list(y[1]))))), sorted(x.cogroup(y).collect()))
[(0, ([0], [0])), (1, ([1], [1])), (2, ([], [2])), (3, ([], [3])), (4, ([2], [4]))]
repartition(self, numPartitions)
source code 
Return a new RDD that has exactly numPartitions partitions.

Can increase or decrease the level of parallelism in this RDD. Internally, this uses a shuffle to redistribute data. If you are decreasing the number of partitions in this RDD, consider using `coalesce`, which can avoid performing a shuffle.

>>> rdd = sc.parallelize([1,2,3,4,5,6,7], 4)
>>> sorted(rdd.glom().collect())
[[1], [2, 3], [4, 5], [6, 7]]
>>> len(rdd.repartition(2).glom().collect())
2
>>> len(rdd.repartition(10).glom().collect())
10
coalesce(self, numPartitions, shuffle=False)
source code 
Return a new RDD that is reduced into `numPartitions` partitions.

>>> sc.parallelize([1, 2, 3, 4, 5], 3).glom().collect()
[[1], [2, 3], [4, 5]]
>>> sc.parallelize([1, 2, 3, 4, 5], 3).coalesce(1).glom().collect()
[[1, 2, 3, 4, 5]]
zip(self, other)
source code 
Zips this RDD with another one, returning key-value pairs with the first element in each RDD second element in each RDD, etc. Assumes that the two RDDs have the same number of partitions and the same number of elements in each partition (e.g. one was made through a map on the other).

>>> x = sc.parallelize(range(0,5))
>>> y = sc.parallelize(range(1000, 1005))
>>> x.zip(y).collect()
[(0, 1000), (1, 1001), (2, 1002), (3, 1003), (4, 1004)]
zipWithIndex(self)
source code 
Zips this RDD with its element indices.

The ordering is first based on the partition index and then the ordering of items within each partition. So the first item in the first partition gets index 0, and the last item in the last partition receives the largest index.

This method needs to trigger a spark job when this RDD contains more than one partitions.

>>> sc.parallelize(["a", "b", "c", "d"], 3).zipWithIndex().collect()
[('a', 0), ('b', 1), ('c', 2), ('d', 3)]
zipWithUniqueId(self)
source code 
Zips this RDD with generated unique Long ids.

Items in the kth partition will get ids k, n+k, 2*n+k, ..., where n is the number of partitions. So there may exist gaps, but this method won't trigger a spark job, which is different from zipWithIndex

>>> sc.parallelize(["a", "b", "c", "d", "e"], 3).zipWithUniqueId().collect()
[('a', 0), ('b', 1), ('c', 4), ('d', 2), ('e', 5)]
setName(self, name)
source code 
Assign a name to this RDD.

>>> rdd1 = sc.parallelize([1,2])
>>> rdd1.setName('RDD1')
>>> rdd1.name()
'RDD1'
getStorageLevel(self)
source code 
Get the RDD's current storage level.

>>> rdd1 = sc.parallelize([1,2])
>>> rdd1.getStorageLevel()
StorageLevel(False, False, False, False, 1)
>>> print(rdd1.getStorageLevel())
Serialized 1x Replicated

   Home   	   Trees   	   Indices   	   Help   	
Spark 1.1.0 Python API Docs
Generated by Epydoc 3.0.1 on Mon Nov 24 15:21:12 2014	http://epydoc.sourceforge.net
