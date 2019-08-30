package day5

object SortRules {

    implicit object OrderingXianRou extends Ordering[XianRou]{
        override def compare(x: XianRou, y: XianRou): Int = {
            if(x.fv==y.fv){
                x.age-y.age
            }else{
                y.fv-x.fv
            }
        }
    }
}
