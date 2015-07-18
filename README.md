# NI Assembly Open Data with Sparkling and Clojure.

For information on the NI Assembly Open Data please have a look at [http://data.niassembly.gov.uk](http://data.niassembly.gov.uk) the API is excellent and well documented.

The codebase is work in progress and changes from time to time, nothing too drastic.

If anything it's a good start to show how Spark is used within Clojure code using the [Gorilla Labs Sparkling Library](https://github.com/gorillalabs/sparkling)

## Getting Started
Fire up your REPL of choice and first copy/paste the Spark config and context lines in the comment block. You'll see them load up from the REPL, as soon as you have a Spark context you are ready to rock.

### Loading the Members
```mlas.core>(def members (load-members sc members-path))```

### Bulk Loading the Questions via URL
At present (though it will change) the personId is not present in the questions API. There's a function to load slurp these files in one by one for each member saving the JSON file in the resources/questions folder. 

```mlas.core>(save-question-data members)```

This may take some times to load them in as they can be quite large.

To load them in as RDD's as it stands it's best to re-map through the members list for the personId key and load the files in as PairRDD.

```mlas.core>(def questions (load-questions members)))```

With the members PairRDD [personid, member-record]) and the questions PairRDD [personid, questions] it's easy to do a join.

```mlas.core>(def member-questions (spark/join members questions))

This will give you a PairRDD of [personid, [member-record, questions]] then you have a basis for counting and analysis.

* Who asked the most questions?
* What's the answered/non-answered ratio?
* What is the term frequency/inverse document frequency (TF/IDF) of the questions asked?

Plenty..... 



