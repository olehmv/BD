package module4.homework1

case class EnrichedItem(motelId: String, motelName: String, bidDate: String, loSa: String, price: Double) {

  override def toString: String = s"$motelId,$motelName,$bidDate,$loSa,$price"


}