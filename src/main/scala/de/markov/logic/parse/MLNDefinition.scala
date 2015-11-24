package de.markov.logic.parse

/**
  * Created by visenger on 20/11/15.
  */
class Domain {

}

trait Dom

trait Dom2

trait Dom3

trait Dom4

trait TypeDef

trait PredicateDefinition

case class IntegerTypeDef(name: String, values: Seq[Int]) extends TypeDef

case class ConstantTypeDef(name: String, constants: Seq[String]) extends TypeDef

case class Predicate1(predicateName: String, domainName: String) extends PredicateDefinition

case class Predicate2(predicateName: String, domainName1: String, domainName2: String) extends PredicateDefinition

case class Variable(varName: String, domainName: String)


