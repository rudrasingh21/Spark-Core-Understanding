*****************************-:Spark DataFrames:-*****************************

Ref:- https://github.com/rich-iannone/so-many-pyspark-examples/blob/master/spark-dataframes.ipynb

Imports
For these examples, we just need to import two pyspark.sql libraries:

types
functions

from pyspark.sql.types import *  # Necessary for creating schemas
from pyspark.sql.functions import * # Importing PySpark functions

*********************************

Creating DataFrame from RDD
I am following these steps for creating a DataFrame from list of tuples:

Create a list of tuples. Each tuple contains name of a person with age.
Create a RDD from the list above.
Convert each tuple to a row.
Create a DataFrame by applying createDataFrame on RDD with the help of sqlContext.
from pyspark.sql import Row
l = [('Ankit',25),('Jalfaizy',22),('saurabh',20),('Bala',26)]
rdd = sc.parallelize(l)
people = rdd.map(lambda x: Row(name=x[0], age=int(x[1])))
schemaPeople = sqlContext.createDataFrame(people)

*********************************

---------------------------------------------------------------
Creating DataFrames Making a Simple DataFrame from a Tuple List:-
---------------------------------------------------------------

>>> l
[('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)]

>>> df_without_schema = sqlContext.createDataFrame(l)

>>> df_without_schema
DataFrame[_1: string, _2: bigint]

>>> type(df_without_schema)
<class 'pyspark.sql.dataframe.DataFrame'>

>>> print df_without_schema
DataFrame[_1: string, _2: bigint]

>>> print df_without_schema.collect()
[Row(_1=u'Ankit', _2=25), Row(_1=u'Jalfaizy', _2=22), Row(_1=u'saurabh', _2=20), Row(_1=u'Bala', _2=26)]

>>> print df_without_schema.show()
+--------+---+
|      _1| _2|
+--------+---+
|   Ankit| 25|
|Jalfaizy| 22|
| saurabh| 20|
|    Bala| 26|
+--------+---+

None

----------------------------------------------------------
Making a Simple DataFrame from a Tuple List and a Schema
----------------------------------------------------------


>>> df_with_schema = sqlContext.createDataFrame(l,['Name','Age'])

>>> df_with_schema
DataFrame[Name: string, Age: bigint]

>>> df_with_schema.show()
+--------+---+
|    Name|Age|
+--------+---+
|   Ankit| 25|
|Jalfaizy| 22|
| saurabh| 20|
|    Bala| 26|
+--------+---+

>>> df_with_schema.printSchema()
root
 |-- Name: string (nullable = true)
 |-- Age: long (nullable = true)

-------------------------------------------
Making a Simple DataFrame from a Dictionary :-
-------------------------------------------

>>> a_dict
[{'letters': 'a', 'numbers': 1}, {'letters': 'b', 'numbers': 2}, {'letters': 'c', 'numbers': 3}]

>>> df_from_dict = (sqlContext.createDataFrame(a_dict))

>>> df_a_dict
DataFrame[letters: string, numbers: bigint]

>>> df_from_dict.show()
+-------+-------+
|letters|numbers|
+-------+-------+
|      a|      1|
|      b|      2|
|      c|      3|
+-------+-------+

--------------------------------------------------------
Making a Simple DataFrame Using a StructType Schema + RDD
--------------------------------------------------------

NOTE:- schema = StructType([StructField('col_name', StringType(), True)])


>>> schema = StructType([StructField('letters', StringType(), True),StructField('numbers', IntegerType(), True)])

>>> l
[('Ankit', 25), ('Jalfaizy', 22), ('saurabh', 20), ('Bala', 26)]

>>> rdd = sc.parallelize(l)

>>> type(schema)
<class 'pyspark.sql.types.StructType'>

>>> type(rdd)
<class 'pyspark.rdd.RDD'>

# Create the DataFrame from these raw components
nice_df = (sqlContext.createDataFrame(rdd, schema))

>>> nice_df.show()
+--------+-------+
| letters|numbers|
+--------+-------+
|   Ankit|     25|
|Jalfaizy|     22|
| saurabh|     20|
|    Bala|     26|
+--------+-------+


Simple Inspection Functions:---

>>> nice_df.columns
['letters', 'numbers']

>>> nice_df.dtypes
[('letters', 'string'), ('numbers', 'int')]

# `printSchema()`: prints the schema of the supplied DF

>>> nice_df.printSchema()
root
 |-- letters: string (nullable = true)
 |-- numbers: integer (nullable = true)

# `schema`: returns the schema of the provided DF as `StructType` schema
>>> nice_df.schema
StructType(List(StructField(letters,StringType,true),StructField(numbers,IntegerType,true)))

>>> print nice_df.first()
Row(letters=u'Ankit', numbers=25)

>>> print nice_df.head()
Row(letters=u'Ankit', numbers=25)

>>> print nice_df.head(2)
[Row(letters=u'Ankit', numbers=25), Row(letters=u'Jalfaizy', numbers=22)]

>>> print nice_df.head(3)
[Row(letters=u'Ankit', numbers=25), Row(letters=u'Jalfaizy', numbers=22), Row(letters=u'saurabh', numbers=20)]

# `count()`: returns a count of all rows in DF
>>> nice_df.count()
4


# `describe()`: print out stats for numerical columns# `descr

>>> nice_df.describe().show()
+-------+------------------+
|summary|           numbers|
+-------+------------------+
|  count|                 4|
|   mean|             23.25|
| stddev|2.7537852736430515|
|    min|                20|
|    max|                26|
+-------+------------------+

# the `explain()` function explains the under-the-hood evaluation process
>>> nice_df.explain()

== Physical Plan ==
Scan ExistingRDD[letters#12,numbers#13]

****************
Some Operations:-
****************

Relatively Simple DataFrame Manipulation Functions¶
Let's use these functions:-

unionAll()	: combine two DataFrames together
orderBy()	: perform sorting of DataFrame columns
select()	: select which DataFrame columns to retain
drop()		: select a single DataFrame column to remove
filter()	: retain DataFrame rows that match a condition

>>> nice_df.unionAll(nice_df)
DataFrame[letters: string, numbers: int]

# Take the DataFrame and add it to itself

>>> nice_df.unionAll(nice_df).show()
+--------+-------+
| letters|numbers|
+--------+-------+
|   Ankit|     25|
|Jalfaizy|     22|
| saurabh|     20|
|    Bala|     26|
|   Ankit|     25|
|Jalfaizy|     22|
| saurabh|     20|
|    Bala|     26|
+--------+-------+

# Add it to itself twice
>>> nice_df.unionAll(nice_df).unionAll(nice_df).show()
+--------+-------+
| letters|numbers|
+--------+-------+
|   Ankit|     25|
|Jalfaizy|     22|
| saurabh|     20|
|    Bala|     26|
|   Ankit|     25|
|Jalfaizy|     22|
| saurabh|     20|
|    Bala|     26|
|   Ankit|     25|
|Jalfaizy|     22|
| saurabh|     20|
|    Bala|     26|
+--------+-------+

>>> nice_df.select(['numbers','letters']).show()
+-------+--------+
|numbers| letters|
+-------+--------+
|     25|   Ankit|
|     22|Jalfaizy|
|     20| saurabh|
|     26|    Bala|
+-------+--------+

# Coercion will occur if schemas don't align
nice_df .select(['numbers', 'letters']).unionAll(nice_df).show()

+--------+--------+
| numbers| letters|
+--------+--------+
|      25|   Ankit|
|      22|Jalfaizy|
|      20| saurabh|
|      26|    Bala|
|   Ankit|      25|
|Jalfaizy|      22|
| saurabh|      20|
|    Bala|      26|
+--------+--------+

>>> nice_df.select(['numbers','letters']).printSchema()
root
 |-- numbers: integer (nullable = true)
 |-- letters: string (nullable = true)

---------------------------------------------
Sorting the DataFrame by the `numbers` column
---------------------------------------------

>>> nice_df.unionAll(nice_df).unionAll(nice_df).show()
+--------+-------+
| letters|numbers|
+--------+-------+
|   Ankit|     25|
|Jalfaizy|     22|
| saurabh|     20|
|    Bala|     26|
|   Ankit|     25|
|Jalfaizy|     22|
| saurabh|     20|
|    Bala|     26|
|   Ankit|     25|
|Jalfaizy|     22|
| saurabh|     20|
|    Bala|     26|
+--------+-------+

>>> nice_df.unionAll(nice_df).unionAll(nice_df).orderBy('numbers',ascending = False).show()
+--------+-------+
| letters|numbers|
+--------+-------+
|    Bala|     26|
|    Bala|     26|
|    Bala|     26|
|   Ankit|     25|
|   Ankit|     25|
|   Ankit|     25|
|Jalfaizy|     22|
|Jalfaizy|     22|
|Jalfaizy|     22|
| saurabh|     20|
| saurabh|     20|
| saurabh|     20|
+--------+-------+

>>> nice_df.unionAll(nice_df).unionAll(nice_df).orderBy('numbers',ascending = True).show()
+--------+-------+
| letters|numbers|
+--------+-------+
| saurabh|     20|
| saurabh|     20|
| saurabh|     20|
|Jalfaizy|     22|
|Jalfaizy|     22|
|Jalfaizy|     22|
|   Ankit|     25|
|   Ankit|     25|
|   Ankit|     25|
|    Bala|     26|
|    Bala|     26|
|    Bala|     26|
+--------+-------+

>>> nice_df.select('letters')
DataFrame[letters: string]

>>> nice_df.select('letters').show()
+--------+
| letters|
+--------+
|   Ankit|
|Jalfaizy|
| saurabh|
|    Bala|
+--------+

>>> nice_df.select('numbers').show()
+-------+
|numbers|
+-------+
|     25|
|     22|
|     20|
|     26|
+-------+

>>> nice_df.drop('letters').show()
+-------+
|numbers|
+-------+
|     25|
|     22|
|     20|
|     26|
+-------+

------------------------------------Filters

# The `filter()` function performs filtering of DF rows

# Here is some numeric filtering with comparison operators
# (>, <, >=, <=, ==, != all work)

>>> nice_df.filter(nice_df.numbers > 1).show()
+-------+-------+
|letters|numbers|
+-------+-------+
|  Ankit|     25|
|Saurabh|     22|
|   Amit|     44|
+-------+-------+

>>> nice_df.filter(nice_df.numbers > 25).show()
+-------+-------+
|letters|numbers|
+-------+-------+
|   Amit|     44|
+-------+-------+

>>> nice_df.filter(nice_df.numbers > 22).filter(nice_df.numbers < 44).show()
+-------+-------+
|letters|numbers|
+-------+-------+
|  Ankit|     25|
+-------+-------+

----------------------------------------
The 'groupBy' Function and Aggregations¶
----------------------------------------

The groupBy() function groups the DataFrame using the specified columns, 
then, we can run aggregation on them. The available aggregate functions are:

count()	: counts the number of records for each group
sum()	: compute the sum for each numeric column for each group
min()	: computes the minimum value for each numeric column for each group
max()	: computes the maximum value for each numeric column for each group
avg() or mean(): computes average values for each numeric columns for each group
pivot()	: pivots a column of the current DataFrame and perform the specified aggregation.


-------------------CSV file operation--------------------

>>> nycflights_schema = StructType([ \
...   StructField('year', IntegerType(), True), \
...   StructField('month', IntegerType(), True),\
...   StructField('day', IntegerType(), True),\
...   StructField('dep_time', StringType(), True),\
...   StructField('dep_delay', IntegerType(), True),\
...   StructField('arr_time', StringType(), True),\
...   StructField('arr_delay', IntegerType(), True),\
...   StructField('carrier', StringType(), True),\
...   StructField('tailnum', StringType(), True),\
...   StructField('flight', StringType(), True),  \
...   StructField('origin', StringType(), True),\
...   StructField('dest', StringType(), True),\
...   StructField('air_time', IntegerType(), True),\
...   StructField('distance', IntegerType(), True),\
...   StructField('hour', IntegerType(), True),\
...   StructField('minute', IntegerType(), True)\
...   ])

=================================================================================

http://jsonstudio.com/resources/


>>> df = sqlContext.read.json("world_bank.json")

>>> df.conut()

500

>>> type(df)
<class 'pyspark.sql.dataframe.DataFrame'>

>>> df.columns
['_id', 'approvalfy', 'board_approval_month', 'boardapprovaldate', ....]


>>> df.select("countryname").show()
+--------------------+
|         countryname|
+--------------------+
|Federal Democrati...|
| Republic of Tunisia|
|              Tuvalu|
|   Republic of Yemen|


>>> df.select(df['id'], df['countryname'], df['totalamt']).show()
+-------+--------------------+---------+
|     id|         countryname| totalamt|
+-------+--------------------+---------+
|P129828|Federal Democrati...|130000000|
|P144674| Republic of Tunisia|        0|
|P145310|              Tuvalu|  6060000|
|P144665|   Republic of Yemen|        0|
|P144933|  Kingdom of Lesotho| 13100000|
|P146161|   Republic of Kenya| 10000000|
+-------+--------------------+---------+


>>> df.groupBy("countryname").count().show()

+--------------------+-----+                                                    
|         countryname|count|
+--------------------+-----+
|   Republic of Chile|    1|
|  Republic of Uganda|    4|
|Federal Democrati...|    4|
|  Kingdom of Lesotho|    3|
|East Asia and Pac...|    1|
+--------------------+-----+

NOTE:- Get more example on:-
https://github.com/apache/spark/blob/master/examples/src/main/python/sql/basic.py

NOTE:- Use this data and Practise more examples .

-----------------Running SQL Queries Programmatically-----------------------

The sql function on a SparkSession enables applications to run SQL queries 
programmatically and returns the result as a DataFrame.


Global Temporary View:-

createOrReplaceTempView

createGlobalTempView

----------------------------Inferring the Schema Using Reflection------------------------

Ref:- https://spark.apache.org/docs/2.2.0/sql-programming-guide.html#sql

# Load a text file and convert each line to a Row.

[cloudera@quickstart ~]$ cat people.txt
Michael, 29
Andy, 30
Justin, 19
Rudra, 22
Seema, 10
Ram, 5

>>> from pyspark.sql import Row

>>> lines = sc.textFile("/people.txt")

>>> parts = lines.map(lambda l: l.split(","))


#>>> parts
#PythonRDD[155] at RDD at PythonRDD.scala:43

#>>> parts.take(1)
#[[u'Michael', u' 29']]

#>>> for i in parts.take(3): print(i)
... 
#[u'Michael', u' 29']
#[u'Andy', u' 30']
#[u'Justin', u' 19']

>>> people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

#>>> people.take(3)
#[Row(age=29, name=u'Michael'), Row(age=30, name=u'Andy'), Row(age=19, name=u'Justin')]

#>>> for i in people.take(4): print(i)
#... 
#Row(age=29, name=u'Michael')
#Row(age=30, name=u'Andy')
#Row(age=19, name=u'Justin')
#Row(age=22, name=u'Rudra')

# Infer the schema, and register the DataFrame as a table.

>>> schemaPeople = sqlContext.createDataFrame(people)

>>> schemaPeople.registerTempTable("people")

# SQL can be run over DataFrames that have been registered as a table.

>>> schemaPeople.registerTempTable("people")

>>> teenagers = sqlContext.sql("select name from people where age >= 0 and age <= 25")

#>>> teenagers.take(5)
#[Row(name=u'Justin'), Row(name=u'Rudra'), Row(name=u'Seema'), Row(name=u'Ram')]

>>> teenNames = teenagers.map(lambda i: "Name: " + i.name)

>>> for i in teenNames.collect(): print(i)
... 
Name: Justin
Name: Rudra
Name: Seema
Name: Ram

----------------------------Programmatically Specifying the Schema----------------------------

==> A DataFrame can be created programmatically with three steps.

1) Create an RDD of tuples or lists from the original RDD;
2) Create the schema represented by a StructType matching the structure of tuples or lists in the RDD created in the step 1.
3) Apply the schema to the RDD via createDataFrame method provided by SQLContext.

#==> Import SQLContext and data types

>>> from pyspark.sql import SQLContext
 
>>> from pyspark.sql.types import *

#==> sc is an existing SparkContext.

>>> sqlContext = SQLContext(sc)

#==> Load a text file and convert each line to a tuple.

>>> line = sc.textFile("/people.txt")
 
>>> parts = lines.map(lambda i: i.split(","))

#>>> for i in parts.collect():print(i)
#... 
#[u'Michael', u' 29']
#[u'Andy', u' 30']
#[u'Justin', u' 19']
#[u'Rudra', u' 22']
#[u'Seema', u' 10']
#[u'Ram', u' 5']

 
>>> people = parts.map(lambda i: (p[0], p[1].strip()))

#>>> for i in people.collect():print(i)
#... 
#(u'Michael', u'29')
#(u'Andy', u'30')
#(u'Justin', u'19')
#(u'Rudra', u'22')
#(u'Seema', u'10')
#(u'Ram', u'5')

>>> schemaString = "Name Age"


>>> fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]

>>> schema = StructType(fields)

>>> schemaPeople = sqlContext.createDataFrame(people,schema)

>>> schemaPeople.registerTempTable("tablePeople")


>>> results = sqlContext.sql("select Name from tablePeople")

>>> for i in results.collect(): print (i)
... 
Row(Name=u'Michael')
Row(Name=u'Andy')
Row(Name=u'Justin')
Row(Name=u'Rudra')
Row(Name=u'Seema')
Row(Name=u'Ram')

>>> EName = results.map(lambda i: "Name: " +i.Name)

>>> for i in EName.collect():print(i)
... 
Name: Michael
Name: Andy
Name: Justin
Name: Rudra
Name: Seema
Name: Ram

-----------------------------: Data Sources :-------------------------

--: Data Sources :--

====> Generic Load/Save Functions:-

In the simplest form, the default data source (parquet unless otherwise configured by spark.sql.sources.default) will be used for all operations.

>>> df = sqlContext.read.load("/users.parquet")

>>> type(df)
<class 'pyspark.sql.dataframe.DataFrame'>

>>> df.schema
StructType(List(StructField(name,StringType,false),StructField(favorite_color,StringType,true),StructField(favorite_numbers,ArrayType(IntegerType,false),false)))

>>> df.columns
['name', 'favorite_color', 'favorite_numbers']

>>> df.select("name", "favorite_color").write.save("NameAndColour.parquet")

====> Manually Specifying Options:-

You can also manually specify the data source that will be used along with any extra options that you would like to pass to the data source. Data sources are specified by their fully qualified name (i.e., org.apache.spark.sql.parquet), but for built-in sources you can also use their short names (json, parquet, jdbc). DataFrames of any type can be converted into other types using this syntax.

>>> df = sqlContext.read.load("world_bank.json", format="json")

>>> type(df)
<class 'pyspark.sql.dataframe.DataFrame'>

>>> df.schema

>>> df.columns

>>> df.select("id","countryname").write.save("IDandCountryName.parquet",format="parquet")

--------------------------Run SQL on files directly---------------------

Instead of using read API to load a file into DataFrame and query it, you can also query that file directly with SQL.

>>> df = sqlContext.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")

>>> df=sqlContext.sql("select * from json.`world_bank.json`")

--------------------------------------------------
Reading Data From JSON and performing Operations:-
--------------------------------------------------

>>> df = sqlContext.read.json("/world_bank.json")

>>> df.show()

>>> df.select('countryname','countrycode','totalamt','totalcommamt').show()

+--------------------+-----------+---------+------------+
|         countryname|countrycode| totalamt|totalcommamt|
+--------------------+-----------+---------+------------+
|Federal Democrati...|         ET|130000000|   130000000|
| Republic of Tunisia|         TN|        0|     4700000|
|              Tuvalu|         TV|  6060000|     6060000|
|   Republic of Yemen|         RY|        0|     1500000|
|  Kingdom of Lesotho|         LS| 13100000|    13100000|
|   Republic of Kenya|         KE| 10000000|    10000000|
|   Republic of India|         IN|500000000|   500000000|
|People's Republic...|         CN|        0|    27280000|
|   Republic of India|         IN|160000000|   160000000|
|  Kingdom of Morocco|         MA|200000000|   200000000|

>>> df.count()
500	

How many columns do we have-->

>>> len(df.columns),df.columns

-----------------------------------------------------------------
summary statistics (mean, standard deviance, min ,max , count)-->
-----------------------------------------------------------------

>>> df.describe().show()

+-------+-------------------+--------------------+-------------------+-------------------+--------------------+--------------------+
|summary|           grantamt|         ibrdcommamt|         idacommamt|    lendprojectcost|            totalamt|        totalcommamt|
+-------+-------------------+--------------------+-------------------+-------------------+--------------------+--------------------+
|  count|                500|                 500|                500|                500|                 500|                 500|
|   mean|          4432400.0|           3.28601E7|         3.542136E7|        1.5472408E8|          6.828146E7|          7.271386E7|
| stddev|2.023307257664499E7|1.0891968009875733E8|7.681430978486921E7|4.764210880341054E8|1.2426624790346095E8|1.2347049476800893E8|
|    min|                  0|                   0|                  0|              30000|                   0|               30000|
|    max|          365000000|          1307800000|          600000000|         5170000000|          1307800000|          1307800000|
+-------+-------------------+--------------------+-------------------+-------------------+--------------------+--------------------+

What happens when we specify the name of a categorical / String columns in describe operation -->

>>> df.describe('countrycode').show()

-------------------------------------------
How to find the number of distinct Records:-
-------------------------------------------
Eg:-



train.select('Product_ID').distinct().count()



----------------------------------------------------
Calculate pair wise frequency of categorical columns:-
----------------------------------------------------


train.crosstab('Age', 'Gender').show()


-------------------------------------------------------------
DataFrame which won’t have duplicate rows of given DataFrame:-
-------------------------------------------------------------

train.select('Age','Gender').dropDuplicates().show()

--------------------------------------------
I want to drop the all rows with null value:-
--------------------------------------------

train.dropna().count()
Output:
166821

------------------------------------------------------------------
I want to fill the null values in DataFrame with constant number:-
------------------------------------------------------------------

train.fillna(-1).show(2)

------------------------------------------------
How to find the mean of each age group in train:-
------------------------------------------------

train.groupby('Age').agg({'Purchase': 'mean'}).show()





==> Adding Two Columns Value and creating Thired Column.

-----------
withColumn()
-----------

>>> df.select(('countryname','countrycode','totalamt','totalcommamt').withColumn("Addition",df.totalamt + df.totalcommamt)).show()

+--------------------+-----------+---------+------------+----------+
|         countryname|countrycode| totalamt|totalcommamt|  Addition|
+--------------------+-----------+---------+------------+----------+
|Federal Democrati...|         ET|130000000|   130000000| 260000000|
| Republic of Tunisia|         TN|        0|     4700000|   4700000|
|              Tuvalu|         TV|  6060000|     6060000|  12120000|
|   Republic of Yemen|         RY|        0|     1500000|   1500000|
|  Kingdom of Lesotho|         LS| 13100000|    13100000|  26200000|
|   Republic of Kenya|         KE| 10000000|    10000000|  20000000|
|   Republic of India|         IN|500000000|   500000000|1000000000|
|People's Republic...|         CN|        0|    27280000|  27280000|
|   Republic of India|         IN|160000000|   160000000| 320000000|

-------------------------
Adding String to a column:- 
-------------------------


-------------------------
Renaming a column in DF:- withColumnRenamed()
-------------------------

>>> df.select('countryname','countrycode','totalamt','totalcommamt').withColumnRenamed("totalamt","TOTAL_AMOUNT").show()

+--------------------+-----------+------------+------------+
|         countryname|countrycode|TOTAL_AMOUNT|totalcommamt|
+--------------------+-----------+------------+------------+
|Federal Democrati...|         ET|   130000000|   130000000|
| Republic of Tunisia|         TN|           0|     4700000|
|              Tuvalu|         TV|     6060000|     6060000|
|   Republic of Yemen|         RY|           0|     1500000|
|  Kingdom of Lesotho|         LS|    13100000|    13100000|
|   Republic of Kenya|         KE|    10000000|    10000000|
|   Republic of India|         IN|   500000000|   500000000|
|People's Republic...|         CN|           0|    27280000|

------
Alias:-
------

>>> df.select(df.countryname.alias("Name")).show()

+--------------------+
|                Name|
+--------------------+
|Federal Democrati...|
| Republic of Tunisia|
|              Tuvalu|
|   Republic of Yemen|
|  Kingdom of Lesotho|
|   Republic of Kenya|
|   Republic of India|



