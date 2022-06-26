1. 编写优化规则类 MultiplyOptimizationRule
2. 注册优化规则
   通过spark提供的接口来注册我们编写好的优化规则
   sparkSession.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)
测试结果
我们在命令行中测试一下，我们可以看到 Project 选择的字段中，(cast(id#7L as double) * 1.0) AS id2#12 已经被优化为 cast(id#7L as double) AS id2#14

scala> val df = spark.range(10).selectExpr("id", "concat('wankun-',id) as name")
df: org.apache.spark.sql.DataFrame = [id: bigint, name: string]

scala> val multipliedDF = df.selectExpr("id * cast(1.0 as double) as id2")
multipliedDF: org.apache.spark.sql.DataFrame = [id2: double]

scala> println(multipliedDF.queryExecution.optimizedPlan.numberedTreeString)
00 Project [(cast(id#7L as double) * 1.0) AS id2#12]
01 +- Range (0, 10, step=1, splits=Some(1))

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.expressions._

object MultiplyOptimizationRule extends Rule[LogicalPlan] with Logging {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Multiply(left,right) if right.isInstanceOf[Literal] &&
      right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>
      logInfo("MyRule 优化规则生效")
      left
  }
}

scala> spark.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)
spark.experimental.extraOptimizations: Seq[org.apache.spark.sql.catalyst.rules.Rule[org.apache.spark.sql.catalyst.plans.logical.LogicalPlan]] = List(MultiplyOptimizationRule$@675d209c)

scala>

scala> val multipliedDFWithOptimization = df.selectExpr("id * cast(1.0 as double) as id2")
multipliedDFWithOptimization: org.apache.spark.sql.DataFrame = [id2: double]

scala> println(multipliedDFWithOptimization.queryExecution.optimizedPlan.numberedTreeString)
00 Project [cast(id#7L as double) AS id2#14]
01 +- Range (0, 10, step=1, splits=Some(1))
