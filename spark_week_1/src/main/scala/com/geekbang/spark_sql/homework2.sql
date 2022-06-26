--1.构建一条 SQL，同时 apply 下面三条优化规则：
--  CombineFilters
--  CollapseProject
--  BooleanSimplification

select a.name from (select name,sal from t where 1 = 1 and sal > 1000) a where a.sal < 2000)

--2.构建一条 SQL，同时 apply 下面五条优化规则：
--  ConstantFolding
--  PushDownPredicates
--  ReplaceDistinctWithAggregate
--  ReplaceExceptWithAntiJoin
--  FoldablePropagation

select distinct a.job from (select * from t where sal > 1000  and 1 = 1) a where a.deptno = 10 except select job from t1 where sal > 2000

