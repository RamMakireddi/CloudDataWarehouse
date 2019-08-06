## Goal
    The goal of this project is the build DWH analytical system in AWS cloud environment to analyze song play data for the music streaming startup, Sparkify.
    
## Target Users
    The data analysts and other analytical teams in Sparkify
    
## Source
    The source data is available in S3 in form of JSON files
    
## Target
    The target data will be in Redshift modeled in STAR schema for easy and effective data analysis
    
## Design Approach
   ### ELT(Extract Load Transform)
       -> ELT approache is considered to improve ETL performance. 
       -> Extract json files from S3 and then directly load into Redshift staging area without any transformations. 
       -> Redshift COPY is used to extract date from S3 and load into Redshift staging area
       -> Data is transformed into Dimensions and Facts then loaded into Redshift public schema, Spartify for data analysis, from staging area, spartify_stage. The data is modeled dimensionally using STAR schema approach.
     
   ### STAR Schema
       Fact - songplays
       Dimensions: Time, User, Song & Artist
       
   ### Data Distribution
       In order to achieve most efficient query performance and distribute the data almost equally in all the Redshift cluster nodes, different distribution types are considered for different tables as shown below,
          songplays - DISTKEY (start_time) and SORTKEY(start_time, artist_id, song_id, user_id)
          user - DISTSTYLE all and SORTKEY (user_id)
          song - DISTSTYLE all  and SORTKEY (song_id)
          artist - DISTSTYLE all and SORTKEY (artist_id)
          time - DISTKEY (start_time) and SORTKEY (start_time)
       


Below are the sample queries analysts might run against this designed DWH in AWS Redshift:    
    
## Sample Queries
 ### Top 10 played songs
        select s.title,count(*) as songcount
        from sparkify.songplays sp
        join sparkify.song s 
        on s.song_id=sp.song_id
        group by s.title
        order by songcount desc
        limit 10
    
 ### Top 5 Singers
        select a.name,count(*) as songcount
        from sparkify.songplays sp
        join sparkify.artist a 
            on a.artist_id=sp.artist_id
        group by a.name
        order by songcount desc
        limit 5
    
 ### Top 10 Active Users in 2018
        select a.first_name||' '||a.last_name as username,count(*) as activitycount
        from sparkify.songplays sp
        join sparkify.user a 
            on a.user_id=sp.user_id
        join sparkify.time t
            on t.start_time=sp.start_time and t.year=2018
        group by username
        order by activitycount desc
        limit 10
    
 ### Scripts
         dwh.cfg - populate Redshift configuration details along with ARN role that has read access to S3 and full access to Redshift cluster 
         create_tables.py - run create_tables.py script to drop the tables if exists, create both staging and public schemas and tables.
         etl.py - run this script to run ELT pipelines to extract data from S3 buckets into Redshift staging area and from populate transformed data into Redshift star schema
         