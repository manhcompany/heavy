package com.heavy.etl.utils

import java.util.ServiceLoader
import scala.collection.JavaConverters._


trait Animal {
  def speak()
}

trait AnimalFactory {
  def factory(name: String) : Option[Animal]
}

object AnimalFactory {

  private val factories = ServiceLoader.load(classOf[AnimalFactory]).asScala.toList.map(_.getClass).map(x => x.newInstance())

  def factory(name: String) : Animal = {
    val animals = factories
      .foldLeft(List[Animal]())((x, y) => y.factory(name) match {
        case Some(d) => x :+ d
        case None => x
      })
    if(animals.lengthCompare(1) == 0) {
      animals.head
    } else {
      throw new Exception(s"We have ${animals.size} object for $name")
    }
  }
}

class DuckAnimalFactory extends AnimalFactory {
  class Duck extends Animal {
    override def speak(): Unit = {
      println("Cac")
    }
  }

  override def factory(name: String): Option[Animal] = {
    name match {
      case "Duck" => Some(new Duck())
      case _ => None
    }
  }
}

class DogCatAnimalFactory extends AnimalFactory {
  class Dog extends Animal {
    override def speak(): Unit = {
      println("Gau")
    }
  }

  class Cat extends Animal {
    override def speak(): Unit = {
      println("Meo")
    }
  }

  override def factory(name: String): Option[Animal] = {
    name match {
      case "Dog" => Some(new Dog())
      case "Cat" => Some(new Cat())
      case _ => None
    }
  }
}

object FactoryApplication {
  def main(args: Array[String]): Unit = {
    val duck = AnimalFactory.factory("Dog")
    duck.speak()
  }
}
