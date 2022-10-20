- [NLP Pipeline](#org7b285c7)
  - [Google Collab](#org71db102)
  - [Spark](#org3894418)
  - [User-defined Functions](#org0aaac06)
  - [Stop Words](#org778a8dd)
  - [Term Frequency-Inverse](#org4ac2b9e)
  - [TF-IDF Python](#org6bbf762)



<a id="org7b285c7"></a>

# NLP Pipeline


<a id="org71db102"></a>

## Google Collab

```python
import os
# Find the latest version of spark 3.0  from http://www.apache.org/dist/spark/ and enter as the spark version
# For example:
# spark_version = 'spark-3.0.3'
spark_version = 'spark-3.3.0'
os.environ['SPARK_VERSION']=spark_version

# Install Spark and Java
!apt-get update
!apt-get install openjdk-11-jdk-headless -qq > /dev/null
!wget -q http://www.apache.org/dist/spark/$SPARK_VERSION/$SPARK_VERSION-bin-hadoop3.tgz
!tar xf $SPARK_VERSION-bin-hadoop3.tgz
!pip install -q findspark

# Set Environment Variables
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = f"/content/{spark_version}-bin-hadoop3"

# Start a SparkSession
import findspark
findspark.init()
```


<a id="org3894418"></a>

## Spark

Start Spark session.

```python
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Tokens").getOrCreate()
```

Import and use Tokenizer.

```python
from pyspark.ml.feature import Tokenizer
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
print(tokenizer)
```

    Tokenizer_1604cacf6fb2

Let&rsquo;s say we have the following dataframe.

```python
df = spark.createDataFrame(
    data=[
        (1, "Spark is great"),
        (2, "We are learning Spark"),
        (3, "Spark is better than hadoop")
    ],
    schema=["id", "sentence"]
)
df.show(5)
```

    +---+--------------------+
    | id|            sentence|
    +---+--------------------+
    |  1|      Spark is great|
    |  2|We are learning S...|
    |  3|Spark is better t...|
    +---+--------------------+

The tokenizer uses a transform method that takes a DataFrame as input.

```python
tokenized_df = tokenizer.transform(df)
tokenized_df.show(truncate=False)
```

    +---+---------------------------+---------------------------------+
    |id |sentence                   |words                            |
    +---+---------------------------+---------------------------------+
    |1  |Spark is great             |[spark, is, great]               |
    |2  |We are learning Spark      |[we, are, learning, spark]       |
    |3  |Spark is better than hadoop|[spark, is, better, than, hadoop]|
    +---+---------------------------+---------------------------------+


<a id="org0aaac06"></a>

## User-defined Functions

User-defined functions (UDFs) are functions created by the user to add custom output columns.

Create a function to return the length of a list.

```python
def word_list_length(word_list):
    return len(word_list)
```

    

Next, we&rsquo;ll import the udf function, the col function to select a column to be passed into a function.

```python
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

count_tokens = udf(word_list_length, IntegerType())
```

    

The udf will take in the name of the function as a parameter and the output data type, which is the IntegerType that we just imported.

We repeat the process now. This time we include the function we previously created to get more information about the token result.

```python
tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
tokenized_df = tokenizer.transform(df)
tokenized_df.withColumn("tokens", count_tokens(col("words"))).show(truncate=False)
```

    +---+---------------------------+---------------------------------+------+
    |id |sentence                   |words                            |tokens|
    +---+---------------------------+---------------------------------+------+
    |1  |Spark is great             |[spark, is, great]               |3     |
    |2  |We are learning Spark      |[we, are, learning, spark]       |4     |
    |3  |Spark is better than hadoop|[spark, is, better, than, hadoop]|5     |
    +---+---------------------------+---------------------------------+------+


<a id="org778a8dd"></a>

## Stop Words

Stop words are words with little to no linguistic value in NLP. Removing them from the data can improve accuracy of the language model. Examples are: &ldquo;a&rdquo;, &ldquo;the&rdquo;, &ldquo;and&rdquo;.

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("StopWords").getOrCreate()
```

    22/10/20 13:38:42 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.

We make a df.

```python
df = spark.createDataFrame([
    (0, ["Big", "data", "is", "super", "fun"]),
    (1, ["This", "is", "going", "to", "be", "epic"])
], ["id", "raw"])
df.show(truncate=False)
```

    +---+-------------------------------+
    |id |raw                            |
    +---+-------------------------------+
    |0  |[Big, data, is, super, fun]    |
    |1  |[This, is, going, to, be, epic]|
    +---+-------------------------------+

Now we import and run the word remover.

```python
from pyspark.ml.feature import StopWordsRemover

remover = StopWordsRemover(inputCol="raw", outputCol="filtered")
remover.transform(df).show(truncate=False)
```

    +---+-------------------------------+-----------------------+
    |id |raw                            |filtered               |
    +---+-------------------------------+-----------------------+
    |0  |[Big, data, is, super, fun]    |[Big, data, super, fun]|
    |1  |[This, is, going, to, be, epic]|[going, epic]          |
    +---+-------------------------------+-----------------------+


<a id="org4ac2b9e"></a>

## Term Frequency-Inverse

Term Frequency-Inverse Document Frequency Weight (TF-IDF) is a statistical weight showing the importance of a word in a document. TF measures the frequency of a word occurring in a document. IDF measures the significance of a word across a set of documents. Multiplyng the two fives us the TF-IDF.

Example.

If &ldquo;Python&rdquo; appears 20 times in a 1,000-word article, the TF weight would be 20 divided by 1,000 which equals 0.02. Assume that this article resides in a database with other articles, totaling 1,000,000 articles.

DF takes the number of documents that contain the word Python and compares to the total number of documents, then takes the logarithm of the results. If Python is mentioned in 1000 articles and the total amount of articles in the database is 1,000,000, the IDF would be the log of the total number of articles divided by the number of articles that contain the word Python.

> IDF = log(total articles / articles that contain the word Python) IDF = log(1,000,000 / 1000) = 3

Now that we have both numbers, we can determine the TF-IDF which is the multiple of the two.

> TF \* IDF = 0.2 \* 3 = .6

There are two ways to convert text to numeric data.

-   A `CountVectorizer` indexes the words accross all documents and returns a vector of word counts correponding to the indeces. They are assigned in descending order of frequency. For example, the word with the highest frequency across all documents will be given an index of 0, and the word with the lowest frequency will have an index equal to the number of words in the text.

-   A `HashingTF` converts words to numeric IDs, where the same words are assigned to the same ID and counted then a vector is returned.


<a id="org6bbf762"></a>

## TF-IDF Python

We are going to import the previous Tokenizer classes plus the HashingTF.

```python
from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover

spark = SparkSession.builder.appName("TF-IDF").getOrCreate()
```

    22/10/20 14:00:10 WARN SparkSession: Using an existing Spark session; only runtime SQL configurations will take effect.

Then we will load data from the web.

```python
from pyspark import SparkFiles
url ="https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-online/module_16/airlines.csv"
spark.sparkContext.addFile(url)
df = spark.read.csv(SparkFiles.get("airlines.csv"), sep=",", header=True)

df.show()
```

    +--------------------+
    |      Airline Tweets|
    +--------------------+
    |@VirginAmerica pl...|
    |@VirginAmerica se...|
    |@VirginAmerica do...|
    |@VirginAmerica Ar...|
    |@VirginAmerica aw...|
    +--------------------+

We tokenize the dataframe.

```python
tokened = Tokenizer(inputCol="Airline Tweets", outputCol="words")
tokened_transformed = tokened.transform(df)
tokened_transformed.show()
```

    +--------------------+--------------------+
    |      Airline Tweets|               words|
    +--------------------+--------------------+
    |@VirginAmerica pl...|[@virginamerica, ...|
    |@VirginAmerica se...|[@virginamerica, ...|
    |@VirginAmerica do...|[@virginamerica, ...|
    |@VirginAmerica Ar...|[@virginamerica, ...|
    |@VirginAmerica aw...|[@virginamerica, ...|
    +--------------------+--------------------+

Then we remove stop-words.

```python
remover = StopWordsRemover(inputCol="words", outputCol="filtered")
removed_frame = remover.transform(tokened_transformed)
removed_frame.show(truncate=False)
```

    +---------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------+
    |Airline Tweets                                                                                                                         |words                                                                                                                                                          |filtered                                                                                       |
    +---------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------+
    |@VirginAmerica plus you've added commercials to the experience... tacky.                                                               |[@virginamerica, plus, you've, added, commercials, to, the, experience..., tacky.]                                                                             |[@virginamerica, plus, added, commercials, experience..., tacky.]                              |
    |@VirginAmerica seriously would pay $30 a flight for seats that didn't have this playing. it's really the only bad thing about flying VA|[@virginamerica, seriously, would, pay, $30, a, flight, for, seats, that, didn't, have, this, playing., it's, really, the, only, bad, thing, about, flying, va]|[@virginamerica, seriously, pay, $30, flight, seats, playing., really, bad, thing, flying, va] |
    |@VirginAmerica do you miss me? Don't worry we'll be together very soon.                                                                |[@virginamerica, do, you, miss, me?, don't, worry, we'll, be, together, very, soon.]                                                                           |[@virginamerica, miss, me?, worry, together, soon.]                                            |
    |@VirginAmerica Are the hours of operation for the Club at SFO that are posted online current?                                          |[@virginamerica, are, the, hours, of, operation, for, the, club, at, sfo, that, are, posted, online, current?]                                                 |[@virginamerica, hours, operation, club, sfo, posted, online, current?]                        |
    |@VirginAmerica awaiting my return phone call, just would prefer to use your online self-service option :(                              |[@virginamerica, awaiting, my, return, phone, call,, just, would, prefer, to, use, your, online, self-service, option, :(]                                     |[@virginamerica, awaiting, return, phone, call,, prefer, use, online, self-service, option, :(]|
    +---------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------+

Now we can use the HashingTF function. It takes input and output column plus a parameter to specify the buckets for the split words. This number must be higher than the number of unique words.

```python
hashing = HashingTF(
    inputCol="filtered",
    outputCol="hashedValues",
    numFeatures=pow(2, 18),
)
hashed_df = hashing.transform(removed_frame)
hashed_df.show(truncate=False)
```

    +---------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------+
    |Airline Tweets                                                                                                                         |words                                                                                                                                                          |filtered                                                                                       |hashedValues                                                                                                                               |
    +---------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------+
    |@VirginAmerica plus you've added commercials to the experience... tacky.                                                               |[@virginamerica, plus, you've, added, commercials, to, the, experience..., tacky.]                                                                             |[@virginamerica, plus, added, commercials, experience..., tacky.]                              |(262144,[1419,99916,168274,171322,186669,256498],[1.0,1.0,1.0,1.0,1.0,1.0])                                                                |
    |@VirginAmerica seriously would pay $30 a flight for seats that didn't have this playing. it's really the only bad thing about flying VA|[@virginamerica, seriously, would, pay, $30, a, flight, for, seats, that, didn't, have, this, playing., it's, really, the, only, bad, thing, about, flying, va]|[@virginamerica, seriously, pay, $30, flight, seats, playing., really, bad, thing, flying, va] |(262144,[30053,44911,70065,94512,99549,145380,166947,171322,178915,229264,237593,239713],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0])|
    |@VirginAmerica do you miss me? Don't worry we'll be together very soon.                                                                |[@virginamerica, do, you, miss, me?, don't, worry, we'll, be, together, very, soon.]                                                                           |[@virginamerica, miss, me?, worry, together, soon.]                                            |(262144,[107065,117975,147224,171322,200547,232735],[1.0,1.0,1.0,1.0,1.0,1.0])                                                             |
    |@VirginAmerica Are the hours of operation for the Club at SFO that are posted online current?                                          |[@virginamerica, are, the, hours, of, operation, for, the, club, at, sfo, that, are, posted, online, current?]                                                 |[@virginamerica, hours, operation, club, sfo, posted, online, current?]                        |(262144,[9641,50671,83962,96266,171322,181964,192171,220194],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0])                                            |
    |@VirginAmerica awaiting my return phone call, just would prefer to use your online self-service option :(                              |[@virginamerica, awaiting, my, return, phone, call,, just, would, prefer, to, use, your, online, self-service, option, :(]                                     |[@virginamerica, awaiting, return, phone, call,, prefer, use, online, self-service, option, :(]|(262144,[6122,50509,50671,67607,98717,121947,128077,171322,225517,234877,261675],[1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0])            |
    +---------------------------------------------------------------------------------------------------------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------------------------------------------------+

The hashedValues will contain the index and term frequency of the corresponding index for each word.

Now we can plug the data into an IDFModel. It will scale the values while down-weighting based on document frequency.

```python
idf = IDF(inputCol="hashedValues", outputCol="features")
idfModel = idf.fit(hashed_df)
rescaledData = idfModel.transform(hashed_df)
rescaledData.select("words", "features").show(truncate=False)
```

    22/10/20 14:07:53 WARN DAGScheduler: Broadcasting large task binary with size 4.0 MiB
    +---------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |words                                                                                                                                                          |features                                                                                                                                                                                                                                                                                                        |
    +---------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
    |[@virginamerica, plus, you've, added, commercials, to, the, experience..., tacky.]                                                                             |(262144,[1419,99916,168274,171322,186669,256498],[1.0986122886681098,1.0986122886681098,1.0986122886681098,0.0,1.0986122886681098,1.0986122886681098])                                                                                                                                                          |
    |[@virginamerica, seriously, would, pay, $30, a, flight, for, seats, that, didn't, have, this, playing., it's, really, the, only, bad, thing, about, flying, va]|(262144,[30053,44911,70065,94512,99549,145380,166947,171322,178915,229264,237593,239713],[1.0986122886681098,1.0986122886681098,1.0986122886681098,1.0986122886681098,1.0986122886681098,1.0986122886681098,1.0986122886681098,0.0,1.0986122886681098,1.0986122886681098,1.0986122886681098,1.0986122886681098])|
    |[@virginamerica, do, you, miss, me?, don't, worry, we'll, be, together, very, soon.]                                                                           |(262144,[107065,117975,147224,171322,200547,232735],[1.0986122886681098,1.0986122886681098,1.0986122886681098,0.0,1.0986122886681098,1.0986122886681098])                                                                                                                                                       |
    |[@virginamerica, are, the, hours, of, operation, for, the, club, at, sfo, that, are, posted, online, current?]                                                 |(262144,[9641,50671,83962,96266,171322,181964,192171,220194],[1.0986122886681098,0.6931471805599453,1.0986122886681098,1.0986122886681098,0.0,1.0986122886681098,1.0986122886681098,1.0986122886681098])                                                                                                        |
    |[@virginamerica, awaiting, my, return, phone, call,, just, would, prefer, to, use, your, online, self-service, option, :(]                                     |(262144,[6122,50509,50671,67607,98717,121947,128077,171322,225517,234877,261675],[1.0986122886681098,1.0986122886681098,0.6931471805599453,1.0986122886681098,1.0986122886681098,1.0986122886681098,1.0986122886681098,0.0,1.0986122886681098,1.0986122886681098,1.0986122886681098])                           |
    +---------------------------------------------------------------------------------------------------------------------------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
