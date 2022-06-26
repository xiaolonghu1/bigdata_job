作业一：为 Spark SQL 添加一条自定义命令
1、在SqlBase.g4添加语法规则
src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBase.g4

statement
| SHOW VERSION
ansiNonReserved
| VERSION
nonReserved
| VERSION
//--SPARK-KEYWORD-LIST-START
VERSION: 'VERSION' | 'V';


2、编译antlr
通过本地maven插件编译

3、编译生成SparkSqlParser.scala
添加一个visitShowVersion()方法

在visitShowVersion()方法中去调用ShowVersionCommand()样例类

此时还需要添加ShowVersionCommand()样例类

ShowVersionCommand()是一个样例类，这样可以直接调用该方法，不需要new

override def visitShowVersion(ctx: ShowVersionContext): LogicalPlan = withOrigin(ctx) {
    ShowVersionCommand()
}

4、创建ShowVersionCommand()样例类

package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType


case class ShowVersionCommand() extends RunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("version", StringType)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sparkVersion = sparkSession.version
    val javaVersion = System.getProperty("java.version")
    val scalaVersion = scala.util.Properties.releaseVersion
    val output = "Spark Version: %s, Java Version: %s, Scala Version: %s"
      .format(sparkVersion, javaVersion, scalaVersion.getOrElse(""))
    Seq(Row(output))
  }
}

build/mvn clean package -DskipTests -Phive -Phive-thriftserver
