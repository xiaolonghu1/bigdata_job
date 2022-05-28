use xiaolong;

create table t_user(
	`userid` int,
	`sex` string,
	`age` int,
	`occupation` int,
	`zipcode` int
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDeLIMITSerDe'
WITH SERDEPROPERTIES ('field.delim'='::')
LOCATION '/xiaolong/hive/data/user/';

create table t_movie
(
	`movieid` int,
	`moviename` string,
	`movietype` string
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDeLIMITSerDe'
WITH SERDEPROPERTIES ('field.delim'='::')
LOCATION '/xiaolong/hive/data/movie';

create table t_rating
(
	`userid` int,
	`movieid` int,
	`rate` int,
	`times`  int
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.contrib.serde2.MultiDeLIMITSerDe'
WITH SERDEPROPERTIES ('field.delim'='::')
LOCATION '/xiaolong/hive/data/rating';

# test 1
SELECT b.age as age, avg(a.rate) as avgrate
FROM `t_rating` a
JOIN `t_user` b on a.userid=b.userid
WHERE a.movieid=2116
GROUP BY b.age;

# test 2
SELECT c.moviename as moviename, avg(a.rate) as avgrate, count(1) as total
FROM `t_rating` a
JOIN `t_user` b on a.userid = b.userid
JOIN `t_movie` c on a.movieid = c.movieid
WHERE b.sex='M'
GROUP BY c.moviename
having total>50
ORDER BY avgrate DESC
LIMIT 10;

# test 3
SELECT a.moviename as moviename, avg(b.rate) as avgrate
FROM `t_movie` a
JOIN `t_rating` b on a.movieid = b.movieid
JOIN (
	SELECT a.userid as userid, count(1) as total
	FROM `t_rating` a
	JOIN `t_user` b on a.userid = b.userid
	WHERE b.sex='F'
	GROUP BY a.userid
	ORDER BY total DESC LIMIT 1
) c on b.userid = c.userid
GROUP BY a.moviename
ORDER BY avgrate DESC LIMIT 10;

