package sparkscala2.test

import scala.collection.mutable.ListBuffer

object test1 {
  def main(args:Array[String]):Unit= {
    
    var subjects = new ListBuffer[String]()
subjects += "sub1"
subjects += "sub2"
subjects += "sub3"

val subList = subjects.toList
println(subList)
    
    
    var subjects1 = scala.collection.mutable.Map[String, String]()
    subjects1 += ("sub1" -> "10")
    subjects1 += ("sub2" -> "20", "sub3" -> "30")
    
    println(subjects1)
  }
  
  
}