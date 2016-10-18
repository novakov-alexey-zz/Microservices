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

      private val buf = {
        val b = mutable.HashMap.empty[T, T]
        b.sizeHint(n)
        b
      }
      var left = n

      override def onPush(): Unit = {
        if (left == n) buf.clear()

        val elem = grab(in)
        buf += elemMapper(elem)
        left -= 1
        if (left == 0) {
          left = n
          push(out, buf.result())
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
      }

      setHandlers(in, out, this)
    }

}
