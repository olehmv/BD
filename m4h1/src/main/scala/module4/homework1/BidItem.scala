package module4.homework1

case class BidItem(motelId: String, bidDate: String, loSa: String, price: Double){

  override def toString: String = s"$motelId,$bidDate,$loSa,$price"

}
