package com.leejean.mysorters

object SortRules {

  implicit object MyOrdering extends Ordering[Teacher] {
    override def compare(x: Teacher, y: Teacher): Int = {

      if(x.fv == y.fv){
        x.age - y.age
      }else{
        y.fv - x.fv
      }

    }
  }

}
