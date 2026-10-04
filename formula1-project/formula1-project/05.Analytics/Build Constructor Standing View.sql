-- Databricks notebook source
select * from formula1.gold.dim_constructors limit 1000 ;

-- COMMAND ----------

select * from formula1.gold.fact_session_results limit 1000 ; 

-- COMMAND ----------

 create or replace view  v_constructor_standing
 as
with base as
(select d.constructor_name as team, d.nationality, fcr.season,
 sum(fcr.points) as points, count_if(fcr.is_win) as wins, count_if(fcr.is_podium) as podium
 from formula1.gold.fact_session_results fcr  join formula1.gold.dim_constructors d
 on fcr.constructor_id = d.constructor_id
 group by d.constructor_name , d.nationality, fcr.season)
 select season, nationality, team, points,wins,podium, row_number() over(partition by season order by points desc, podium desc) as pos
 from base


-- COMMAND ----------

select * from v_constructor_standing