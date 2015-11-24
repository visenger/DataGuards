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

    /*var domainToVarsDictionary: Map[String, Set[String]] = Map()
    var varToDomainDictionary: Map[String, String] = Map()

    formulas.foreach(f => {
      val parsedFormula: Formula = f.get.asInstanceOf[Formula]
      //      val variables = parsedFormula.allVariables
      //      println(variables)

      val predicates = parsedFormula.allPredicates
      println(predicates)

      predicates.foreach(_ match {

        case Atom(name, args) => {
          val predicateDef = if (predicateDefs.contains(name)) predicateDefs.get(name).get
          predicateDef match {
            case Predicate1(predicate, domainName) => {
              var vars: Set[String] = domainToVarsDictionary.getOrElse(domainName, Set())
              val variable: String = args.head.toString
              vars += variable
              domainToVarsDictionary += (domainName -> vars)

              varToDomainDictionary += (variable -> domainName)
            }
            case Predicate2(predicate, domainName1, domainName2) => {
              var vars1: Set[String] = domainToVarsDictionary.getOrElse(domainName1, Set())
              val variable1: String = args.head.toString
              vars1 += variable1
              domainToVarsDictionary += (domainName1 -> vars1)
              varToDomainDictionary += (variable1 -> domainName1)

              var vars2: Set[String] = domainToVarsDictionary.getOrElse(domainName2, Set())
              val variable2: String = args.tail.head.toString
              vars2 += variable2
              domainToVarsDictionary += (domainName2 -> vars2)
              varToDomainDictionary += (variable2 -> domainName2)
            }
          }
        }
      })
    })
*/
    val variablesToDomainName: Map[String, String] = formulas.foldLeft(Map[String, String]()) {
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
                  Seq((variable1 -> domainName1), (variable2 -> domainName2))
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

    val groupedByDomName: Map[String, Map[String, String]] = variablesToDomainName.groupBy { case (varName, domName) => domName }
    val domainToVarsDictionary: Map[String, Set[String]] = groupedByDomName.map(e => {
      val variables: Set[String] = e._2.keySet
      e._1 -> variables
    })
    domainToVarsDictionary




    val db = MLNParser.db
    val db_train_file = scala.io.Source.fromFile(s"$mln_dir/$db_file")
    val filtered_db: Iterator[String] = db_train_file.getLines().filter(nonMLNElements(_))
    val parsed_db = filtered_db map (MLNParser.parse(db, _))
    parsed_db foreach (x => println("parsed train db: " + x))


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
