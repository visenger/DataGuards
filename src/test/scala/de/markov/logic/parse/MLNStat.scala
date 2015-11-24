package de.markov.logic.parse


import de.markov.logic.parse.MLNParser._

/**
  * Created by visenger on 20/11/15.
  */
object MLNStat {

  def main(args: Array[String]): Unit = {
    //todo: add parameters parsing in order to pass data via params.

    val mln_dir = "src/test/data"
    val mln_raw = "smoking.mln"
    val db_file = "smoking-train.db"

    val mln_exp = expression
    val mln_file = scala.io.Source.fromFile(s"$mln_dir/$mln_raw")

    val cleanedMLN: List[String] = mln_file.getLines().filter(nonMLNElements(_)).toList
    val mln_as_list = cleanedMLN map (MLNParser.parse(mln_exp, _))
    mln_as_list foreach (x => println("parsed mln: " + x))

    val predicatesDefinitions = mln_as_list.filter(f => {
      f.get.isInstanceOf[Atom]
    })


    val predicateDefs: Map[String, PredicateDefinition] = predicatesDefinitions.map(a => {
      val pDefinition = a.get match {
        case Atom(predicate, args) => {
          predicate -> specifyPredicateDefinition(predicate, args)
        }
      }
      pDefinition
    }).toMap



    val formulas = mln_as_list.filter(e => e.get.isInstanceOf[Formula] && !e.get.isInstanceOf[Atom])

    /* variable name -> domain*/
    val varsToDomainNameDictionary: Map[String, String] = assignVariablesToDomainNames(formulas, predicateDefs)

    val groupedByDomName: Map[String, Map[String, String]] = varsToDomainNameDictionary.groupBy { case (varName, domName) => domName }

    /* domain name -> set of vars */
    val domainToVarsDictionary: Map[String, Set[String]] = groupedByDomName.map(e => {
      val variables: Set[String] = e._2.keySet
      e._1 -> variables
    })


    val db = MLNParser.db
    val db_train_file = scala.io.Source.fromFile(s"$mln_dir/$db_file")
    val filtered_db: Iterator[String] = db_train_file.getLines().filter(nonMLNElements(_))
    val parsed_db = filtered_db.map(MLNParser.parse(db, _)).toList
    parsed_db foreach (x => println("parsed train db: " + x))

    //todo: gathering domain values
    val separateDbAtomsAndFuncs: (List[MLNParser.ParseResult[Any]], List[MLNParser.ParseResult[Any]])
    = parsed_db.partition(_.get.isInstanceOf[DatabaseAtom])

    val dbAtoms = separateDbAtomsAndFuncs._1.map(_.get.asInstanceOf[DatabaseAtom])

    //todo: enhancing domain with constants from the functions
    val dbFunctions = separateDbAtomsAndFuncs._2.map(_.get.asInstanceOf[DatabaseFunction])

    val atomsGroupedByName: Map[String, List[DatabaseAtom]] = dbAtoms.groupBy(a => a.predicate)

    val domainsByPredicates: Seq[ConstantTypeDef] = extractDomain(atomsGroupedByName, predicateDefs)
    val domainsByName: Map[String, Seq[ConstantTypeDef]] = domainsByPredicates.groupBy(_.name)

    val domNameToVals: Map[String, Seq[String]] = domainsByName.mapValues(d => {
      val domain: Seq[String] = d.foldLeft(Set[String]())(_ ++ _.constants).toSeq
      domain
    })
    val allDomains: List[ConstantTypeDef] = domNameToVals.map(e => ConstantTypeDef(e._1, e._2)).toList

    allDomains


  }

  def assignVariablesToDomainNames(formulas: List[MLNParser.ParseResult[Expression]], predicateDefs: Map[String, PredicateDefinition]): Map[String, String] = {
    formulas.foldLeft(Map[String, String]()) {
      (accumulator, element) => {
        val parsedFormula: Formula = element.get.asInstanceOf[Formula]

        val predicates = parsedFormula.allPredicates
        println(predicates)

        val singleFormulaVals: Map[String, String] = predicates.foldLeft(Map[String, String]()) { (internAcc, predicate) =>

          val valsToDomain: Map[String, String] = predicate match {
            case Atom(name, args) => {
              val predicateDef = if (predicateDefs.contains(name)) predicateDefs.get(name).get
              val bindings: Seq[(String, String)] = predicateDef match {
                case Predicate1(predicate, domainName) => {
                  val variable: String = args.head.toString
                  Seq(variable -> domainName)
                }
                case Predicate2(predicate, domainName1, domainName2) => {
                  val variable1: String = args.head.toString
                  val variable2: String = args.tail.head.toString
                  Seq(variable1 -> domainName1, variable2 -> domainName2)
                }
              }
              bindings.toMap
            }
          }
          internAcc ++ valsToDomain
        }
        accumulator ++ singleFormulaVals
      }
    }
  }

  def extractDomain(atomsGroupedByName: Map[String, List[DatabaseAtom]], predicateDefs: Map[String, PredicateDefinition]): Seq[ConstantTypeDef] = {
    atomsGroupedByName.foldLeft(Seq[ConstantTypeDef]()) { (accumulator, element) => {
      val predicateName: String = element._1
      val definition: PredicateDefinition = predicateDefs.get(predicateName).get

      val domainValues: Seq[ConstantTypeDef] = definition match {
        case Predicate1(name, dom1) => {
          val domain1Values: Seq[String] = element._2.foldLeft(Seq[String]()) {
            (a, e) => a ++ Seq(e.args.head.toString)
          }
          Seq(ConstantTypeDef(dom1, domain1Values))
        }
        case Predicate2(name, dom1, dom2) => {
          val domain1Values: Seq[String] = element._2.foldLeft(Seq[String]()) {
            (a, e1) => a ++ Seq(e1.args.head.toString)
          }
          val domain2Values: Seq[String] = element._2.foldLeft(Seq[String]()) {
            (a, e2) => a ++ Seq(e2.args.tail.head.toString)
          }
          Seq(ConstantTypeDef(dom1, domain1Values), ConstantTypeDef(dom2, domain2Values))
        }
      }

      accumulator ++ domainValues

    }
    }
  }

  def nonMLNElements(x: String): Boolean = {
    /*Methods with empty parameter lists are, by convention, evaluated for their side-effects.
     Methods without parameters are assumed to be side-effect free. That's the convention. */
    !((x startsWith "//") || (x isEmpty))
  }

  def specifyPredicateDefinition(predicate: String, args: List[Term]): PredicateDefinition = {
    val predicateDef: PredicateDefinition = args match {
      case List(arg1) => {
        Predicate1(predicate, arg1.toString)
      }
      case List(arg1, arg2) => {
        Predicate2(predicate, arg1.toString, arg2.toString)
      }
    }
    predicateDef
  }

}
