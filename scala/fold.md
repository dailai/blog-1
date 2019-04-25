```scala
val list: List[Int] = List(1, 3, 5, 7, 9)

list.foldLeft(0)(_ + _)
// This is the only valid order
0 + 1 = 1
        1 + 3 = 4
                4 + 5 = 9
                        9 + 7 = 16
                                16 + 9 = 25 // done
                                
list.foldRight(0)(_ + _)
// This is the only valid order
0 + 9 = 9
        9 + 7 = 16
                16 + 5 = 21
                         21 + 3 = 24
                                  24 + 1 = 25 // done
                                  
list.fold(0)(_ + _) // 25
// One of the many valid orders
0 + 1 = 1    0 + 3 = 3             0 + 5 = 5
        1            3 + 7 = 10            5 + 9 = 14    
        1                    10          +         14 = 24
        1                        +                      24 = 25 // done
```





参考资料

<https://commitlogs.com/2016/09/10/scala-fold-foldleft-and-foldright/>

