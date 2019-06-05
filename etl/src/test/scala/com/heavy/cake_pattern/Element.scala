package com.heavy.cake_pattern

abstract class Element {
  def  content: Array[String]
  def height: Int = content.length
  def width: Int = if(height == 0) 0 else content(0).length
}