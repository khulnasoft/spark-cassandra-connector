package com.khulnasoft.spark.connector

import com.khulnasoft.oss.driver.api.core.data.{TupleValue => DriverTupleValue}
import com.khulnasoft.spark.connector.types.NullableTypeConverter

import scala.reflect.runtime.universe._

final case class TupleValue(values: Any*) extends ScalaGettableByIndexData with Product {
  override def columnValues = values.toIndexedSeq.map(_.asInstanceOf[AnyRef])

  override def productArity: Int = columnValues.size

  override def productElement(n: Int): Any = getRaw(n)
}

object TupleValue {

  def fromJavaDriverTupleValue
      (value: DriverTupleValue)
      : TupleValue = {
    val values =
      for (i <- 0 until value.getType.getComponentTypes.size()) yield
        GettableData.get(value, i)
    new TupleValue(values: _*)
  }

  val TypeTag = typeTag[TupleValue]
  val Symbol = typeOf[TupleValue].asInstanceOf[TypeRef].sym

  implicit object TupleValueConverter extends NullableTypeConverter[TupleValue] {
    def targetTypeTag = TypeTag
    def convertPF = {
      case x: TupleValue => x
    }
  }
}