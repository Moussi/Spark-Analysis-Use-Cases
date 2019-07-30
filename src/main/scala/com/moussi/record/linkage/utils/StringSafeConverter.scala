package com.moussi.record.linkage.utils

import scala.util.{Success, Try}

object StringSafeConverter {

    implicit class SafeConverter(s: String) {
        def toSafeDouble = Try(s.toDouble) match {
            case Success(value) => value
            case _ => 0.0
        }
    }
}
