package service.dataprocessor

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

import scala.collection.mutable

case class UniqueGroup[T](n: Int, elemMapper: T => (T, T)) extends GraphStage[FlowShape[T, mutable.HashMap[T, T]]] {
  require(n > 0, "n must be greater than 0")
  val in = Inlet[T]("Grouped.in")
  val out = Outlet[mutable.HashMap[T, T]]("Grouped.out")

  override val shape: FlowShape[T, mutable.HashMap[T, T]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {
      println("Create new UniqueGroup >>>>")
      UniqueGroup.i += 1
      val id = UniqueGroup.i
      var cycle = 0

      private val buf = {
        val b = mutable.HashMap.empty[T, T]
        b.sizeHint(n)
        b
      }
      var left = n

      override def onPush(): Unit = {
        buf += elemMapper(grab(in))
        left -= 1
        if (left == 0) {
          val elements = buf.result()
          buf.clear()
          left = n
          push(out, elements)
          cycle += 1
        } else {
          pull(in)
        }
      }

      override def onPull(): Unit = {
        pull(in)
      }

      override def onUpstreamFinish(): Unit = {
        // This means the buf is filled with some elements but not enough (left < n) to group together.
        // Since the upstream has finished we have to push them to downstream though.
        if (left < n) {
          left = n
          push(out, buf)
        }
        completeStage()
        println(s"unique group finished # $id, cycles count = $cycle $this" )
      }

      setHandlers(in, out, this)
    }

}

object UniqueGroup {
  var i = 0
}
