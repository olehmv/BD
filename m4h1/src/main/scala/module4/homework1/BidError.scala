package module4.homework1

case class BidError(date: String, errorMessage: String) {

  override def toString: String = s"$date,$errorMessage"
}
