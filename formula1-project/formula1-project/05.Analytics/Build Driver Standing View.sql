-- Databricks notebook source
select * from formula1.gold.dim_drivers limit 1000 ;

-- COMMAND ----------

select * from formula1.gold.fact_session_results limit 1000 ; 

-- COMMAND ----------

 create or replace view  v_driver_standing
 as
with base as
(select d.driver_name as driver, d.nationality, fcr.constructor_id as team, fcr.season,
 sum(fcr.points) as points, count_if(fcr.is_win) as wins, count_if(fcr.is_podium) as podium
 from formula1.gold.fact_session_results fcr  join formula1.gold.dim_drivers d
 on fcr.driver_id = d.driver_id
 group by d.driver_name , d.nationality, fcr.constructor_id , fcr.season)
 select driver, nationality, team, season,points,wins,podium, row_number() over(partition by season order by points desc, podium desc) as pos
 from base


-- COMMAND ----------



-- COMMAND ----------

