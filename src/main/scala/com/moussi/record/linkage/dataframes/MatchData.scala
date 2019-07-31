package com.moussi.record.linkage.dataframes

case class MatchData(
    id_1: Option[Int],
    id_2: Option[Int],
    cmp_fname_c1: Option[Double],
    cmp_fname_c2: Option[Double],
    cmp_lname_c1: Option[Double],
    cmp_lname_c2: Option[Double],
    cmp_sex: Option[Int],
    cmp_bd: Option[Int],
    cmp_bm: Option[Int],
    cmp_by: Option[Int],
    cmp_plz: Option[Int],
    is_match: Boolean
) {
    def scoreMatchData() = {
        (Score(this.cmp_lname_c1.getOrElse(0.0)) + this.cmp_plz + this.cmp_by + this.cmp_bd + this.cmp_bm).value
    }

}

case class Score(value: Double) {
    def +(oi: Option[Int]) = {
        Score(value + oi.getOrElse(0))
    }

}
