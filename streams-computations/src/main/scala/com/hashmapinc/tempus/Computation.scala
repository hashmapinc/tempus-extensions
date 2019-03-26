package com.hashmapinc.tempus

import scala.annotation.tailrec
import scala.collection.immutable
import scala.util.Try

object Computation extends App{

  object OptionsExtractor{
    def apply(s: List[String]): ErrorOrOptions ={

      @tailrec
      def accumulator(l: OptionsMap, i: List[String]): ErrorOrOptions ={
        i match {
          case Nil => Right(l)
          case o :: v :: e if o.startsWith("--") =>
            accumulator(l + (o.replaceFirst("--", "") -> v.asInstanceOf[String]), e)
          case _ => Left(new IllegalArgumentException("Wrong Parameters"))
        }
      }

      accumulator(Map.empty[String, String], s)
    }
  }

  private val argsList: List[String] = args.toList

  OptionsExtractor(argsList) match {
    case Right(opt) =>
      (opt.get("source"), opt.get("computation-class")) match {
        case (Some(s: String), Some(c: String)) if s.equalsIgnoreCase("kafka") =>
          SparkKafkaApplication.run(opt, buildSparkJob(c))
        case (Some(s: String), Some(c: String)) if s.equalsIgnoreCase("kinesis") =>
          SparkKinesisApplication.run(opt, buildSparkJob(c))
        case (Some(s: String), None) => throw new IllegalArgumentException("Missing parameter computation-class")
        case _ => throw new IllegalArgumentException("Missing required parameters source and computation-class")
      }
    case _ => new IllegalArgumentException("Usage")
  }

  def buildSparkJob(className: String): OptionsMap => Try[Job] = { opt: OptionsMap =>
    Try(Class.forName(className)).flatMap{ clazz =>
      Try{
        clazz.getConstructors
          .filter(c => c.getParameterCount == 1 && c.getParameterTypes.contains(classOf[immutable.Map[String, Any]]))
          .map(_.newInstance(opt).asInstanceOf[Job])
          .head
      }
    }
  }

}
