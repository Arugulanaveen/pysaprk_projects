# Databricks notebook source
from pyspark.sql.types import StructField,StructType,IntegerType,StringType,BooleanType,DateType,DecimalType
import  pyspark.sql.functions as F 
from pyspark.sql.window import Window


# COMMAND ----------

from pyspark.sql import SparkSession

#create session

spark=SparkSession.builder.appName("IPL dayta analysis").getOrCreate()

# COMMAND ----------

ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

# COMMAND ----------

ball_by_ball_df=spark.read.schema(ball_by_ball_schema).format("csv").option("header","true").option("inferschema","true").load("s3://ipl-data-analysis-pyspark-project1/Ball_By_Ball.csv")

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

match_df=spark.read.schema(match_schema).format("csv").option("header","true").load("s3://ipl-data-analysis-pyspark-project1/Match.csv")

# COMMAND ----------

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

player_df=spark.read.schema(player_schema).format("csv").option("header","true").load("s3://ipl-data-analysis-pyspark-project1/Player.csv")

# COMMAND ----------

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

player_match_df=spark.read.option("header","true").format("csv").schema(player_match_schema).load("s3://ipl-data-analysis-pyspark-project1/Player_match.csv")

# COMMAND ----------

team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])


team_df=spark.read.format("csv").schema(team_schema).option("header","true").load("s3://ipl-data-analysis-pyspark-project1/Team.csv")

# COMMAND ----------

#filtering the valid balls only
ball_by_ball_df=ball_by_ball_df.filter((F.col("wides")==0) & (F.col("noballs")==0))

##avg and total run per  an innings
total_and_avg_runs_per_innings=ball_by_ball_df.groupBy("match_id","innings_no").agg(F.sum("runs_scored").alias("total_runs"),F.avg("runs_scored").alias("avg_runs"))

# COMMAND ----------

windowSpec=Window.partitionBy("match_id","innings_no").orderBy("over_id")

ball_by_ball_df=ball_by_ball_df.withColumn("running_total_runs",F.sum("runs_scored").over(windowSpec))


# COMMAND ----------

ball_by_ball_df=ball_by_ball_df.withColumn("high_impact",F.when(((F.col("runs_scored")+ F.col("extra_runs"))>6) | (F.col("bowler_wicket")==True),True).otherwise(False))

# COMMAND ----------

from pyspark.sql.functions import year,month,dayofmonth

#extracting year,month,day
match_df=match_df.withColumn("year",year("match_date"))
match_df=match_df.withColumn("month",year("match_date"))
match_df=match_df.withColumn("day",dayofmonth("match_date"))

#high margin wins

match_df=match_df.withColumn("win_margin_category",F.when(F.col("win_margin")>=100,"High").when((F.col("win_margin")>=50) & (F.col("win_margin")<100),"medium").otherwise("low"))


match_df=match_df.withColumn("toss_match_winner",F.when(F.col("toss_winner")==F.col("match_winner"),"Yes").otherwise("No"))




# COMMAND ----------

# clean player names
player_df=player_df.withColumn("player_name",F.regexp_replace("player_name","[^a-zA-Z0-9]",""))
#handling missing values properly
player_df=player_df.na.fill({"batting_hand":"unknown","bowling_skill":"unknown"})
#categorise batters based on batting style
player_df=player_df.withColumn("batting_style",F.when(F.col("batting_hand").contains("Left"),"Left-Handed").otherwise("Right-Handed"))


# COMMAND ----------

#years since debut
player_match_df=player_match_df.withColumn("years_since_debut",year(F.current_date())-F.col("season_year"))

#chek if veteran
player_match_df=player_match_df.withColumn("veteran_status",F.when(F.col("age_as_on_match")>=35,"Veteran").otherwise("Non-veteran"))

# COMMAND ----------

player_match_df.show(5)

# COMMAND ----------

ball_by_ball_df.createOrReplaceTempView("ball_by_ball")
match_df.createOrReplaceTempView("match")
player_df.createOrReplaceTempView("player")
player_match_df.createOrReplaceTempView("player_match")
team_df.createOrReplaceTempView("team")

# COMMAND ----------

# DBTITLE 1,;;;
top_scoring_batsmen_per_season=spark.sql('''
                                         select
                                         pm.player_name,
                                         m.season_year,
                                         rank() over(partition by m.season_year order by sum(b.runs_scored) desc )  as rank
                                         ,sum(b.runs_scored) as total_runs
                                         from ball_by_ball b
                                         join  match m on b.match_id =m.match_id
                                         join player_match pm on b.match_id=pm.match_id and b.striker=pm.player_id
                                         group by m.season_year,pm.player_name
                                         order by total_runs desc
                                         ''').where("rank==1")

# COMMAND ----------

economical_powerplay_bowlers=spark.sql(
    '''
    select
    pm.player_name
    ,m.season_year
    ,avg(b.runs_scored) as avg_runs_per_ball
    ,rank() over(partition by m.season_year order by avg(b.runs_scored) asc ) as rank
    ,count(*) as no_of_balls
     from (select match_id,bowler,runs_scored ,bowler_wicket from  ball_by_ball where over_id <=6 ) b
     join player_match pm on b.match_id=pm.match_id and b.bowler=pm.player_id
     join match m on b.match_id=m.match_id
     group by pm.player_name ,m.season_year
    --  having count(b.bowler_wicket)>=1
     order by avg_runs_per_ball asc

    '''
).where("rank==1")
economical_powerplay_bowlers.show()

# COMMAND ----------

percentage_of_games_with_toss_and_match_win=spark.sql('''
                      select
                      sum(case when match_winner=toss_winner then 1 else 0 end )/count(*)*100 as toss_and_match_percentage
                       from match m 
                       where m.toss_name is not null
                    
                            ''')
percentage_of_games_with_toss_and_match_win.show()