# Simplified-Query-Processor

PARTITIONING THE TABLE:

1. ImplementaPythonfunctionLoadRatings()thattakesafilesystempaththatcontainstherating.dat file as input. Load Ratings() then      load the rating.dat content into a table (saved in PostgreSQL) named Ratings that has the following schema
                                          UserID - MovieID - Rating

2. Implement a Python function Range Partition() that takes as input: (1) the Ratings table stored in PostgreSQL and (2) an      integer value N; that represents the number of partitions. Range Partition() then generates N horizontal fragments of the      Ratings table and store them in PostgreSQL. The algo- rithm should partition the ratings table based on N uniform ranges of    the Rating attribute.

3. Implement a Python function RoundRobin Partition() that takes as input: (1) the Ratings table stored in PostgreSQL and (2)    an integer value N; that represents the number of partitions. The function then generates N horizontal fragments of the        Ratings table and stores them in PostgreSQL. The algorithm should partition the ratings table using the round robin            partitioning approach

4. Implement a Python function RoundRobin Insert() that takes as input: (1) Ratings table stored in PostgreSQL, (2) UserID,      (3) ItemID, (4) Rating. RoundRobin Insert() then inserts a new tuple in the right fragment (of the partitioned ratings        table) based on the round robin approach.

5. Implement a Python function Range Insert() that takes as input: (1) Ratings table stored in Post- greSQL (2) UserID, (3)      ItemID, (4) Rating. Range Insert() then inserts a new tuple in the correct fragment (of the partitioned ratings table)        based upon the Rating value.

6. Implement a Python function Delete Partitions() that deletes all generated partitions as well as any metadata related to      the partitioning scheme.

NOTE: Download rating.dat file from the MovieLens website (http://files.grouplens.org/datasets/movielens/ml- 10m.zip)
                                           

**Range Query:**

RangeQuery() –

1. Implemented a Python function RangeQuery that takes as input: (1) Ratings table stored in PostgreSQL, (2) RatingMinValue      (3) RatingMaxValue (4) openconnection.
2. Please note that the RangeQuery would not use ratings table but it would use the range and round robin partitions of the      ratings table.
3. RangeQuery() then returns all tuples for which the rating value is larger than or equal to RatingMinValue and less than or    equal toRatingMaxValue.
4. The returned tuples should are stored in a text file, named RangeQueryOut.txt such that each line represents a tuple that      has the following format such that PartitionName represents the full name of the partition i.e. RangeRatingsPart1 or          RoundRobinRatingsPart4 etc. in which this tuple resides.

**Example:**

**PartitionName, UserID, MovieID, Rating**

RangeRatingsPart0,1,377,0.5
RoundRobinRatingsPart1,1,377,0.5

Note: Please use ‘,’ (COMMA, no space character) as delimiter between PartitionName, UserID, MovieID and Rating.

**Point Query:**

PointQuery() –
1. Implement a Python function PointQuery that takes as input: (1) Ratings table stored in PostgreSQL, (2) RatingValue.          (3)openconnection
2. Please note that the PointQuery would not use ratings table but it would use the range and round robin partitions of the      ratings table.
3. PointQuery() then returns all tuples for which the rating value is equal to RatingValue.
4. The returned tuples should be stored in a text file, named PointQueryOut.txt such that each line represents a tuple that      has the following format such that PartitionName represents the full name of the partition i.e. RangeRatingsPart1 or          RoundRobinRatingsPart4 etc. in which this tuple resides.

**Example:** 

**PartitionName, UserID, MovieID, Rating RangeRatingsPart3,23,459,3.5**

RoundRobinRatingsPart4,31,221,0

Note: Please use ‘,’ (COMMA) as delimiter between PartitionName, UserID, MovieID and Rating.
                                                                 
                                                                
