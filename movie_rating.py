# This pyspark program stages 3 datasets . Joins datasets and evaluated the highest rated genres for every year available in the past decade (2000 -2010)
#Following assumptions were made regarding the data
#    1.Movie records are unique in the dataset
#    2. If the movie has multiple genres, then rating is applicable for all the genres to which movie belongs 
#    3. Business logic used to caluating the highest rating is based on summing all the ratings by genre and selecting the max value for a given year
#    4. WeI have considred rating year for calculation by deriving from rating timestamp not the movie release year from movie title

##Importing libraries##
from pyspark.sql.functions import split, explode
from pyspark.sql import SQLContext
from pyspark.sql.functions import from_unixtime, unix_timestamp
sqlcontext = SQLContext(sc)

# File location for data source files
users_file_location = "dbfs:/FileStore/tables/users_dat.txt"
movies_file_location = "dbfs:/FileStore/tables/movies_dat.txt"
ratings_file_location = "dbfs:/FileStore/tables/ratings_dat.txt"

# Creating rdd's and dataframes by reading data source files
users_rdd = sc.textFile(users_file_location)
users_df=users_rdd.map(lambda k: k.split("::")).toDF(['user_id','twitter_id'])
users_df.cache().createOrReplaceTempView("users")

movies_rdd = sc.textFile(movies_file_location)
movies_df=movies_rdd.map(lambda k: k.split("::")).toDF(['movie_id','movie_title','genre'])

ratings_rdd = sc.textFile(ratings_file_location)
ratings_df1=ratings_rdd.map(lambda k: k.split("::")).toDF(['user_id','movie_id','rating','rating_timestamp'])
ratings_df=ratings_df1.withColumn("rating_timestamp_datetime",from_unixtime(ratings_df1.rating_timestamp)).withColumn("rating_year",from_unixtime(ratings_df1.rating_timestamp)[0:4])

# joining ratings dataset witht he movies dataset to get all the required columns for analysis in single dataframe
join_df = ratings_df.join(movies_df,ratings_df.movie_id == movies_df.movie_id).select(ratings_df["*"],movies_df["movie_title"],movies_df["genre"],explode(split(movies_df["genre"], "\|")).alias("genre_split"))
join_df.cache().createOrReplaceTempView("movie_genre_rating")

# running sql on table created in above step to find the most popular genre year by year in the past decade
op_df = sqlcontext.sql("select rating_year,genre_split as most_popular_genre from (select *,dense_rank() over(partition by rating_year order by rating_sum desc) as rn from (select rating_year,genre_split,sum(rating) as rating_sum from movie_genre_rating where rating_year >=2010 group by rating_year,genre_split)a)b where rn=1")

op_df.orderBy("rating_year").show()

