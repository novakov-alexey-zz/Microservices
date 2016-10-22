package service.dataprocessor.dal

import java.time.LocalDateTime

import org.mybatis.scala.mapping.{Insert, Statement}
import service.dataprocessor.Modules

trait EventDao {
  def insertEvent(e: Event)
}

class EventDaoImpl extends EventDao {
  val db = Modules.persistenceContext

  override def insertEvent(e: Event): Unit =
    db.transaction { implicit session => EventDaoMapping.insertEvent(e) }
}

object EventDaoMapping {
  val insertEvent = new Insert[Event] {
    override def xsql =
      <xsql>
        INSERT INTO dbo.Event (EVENT_ID, NAME, TIME_OF_START, FILE_NAME)
        VALUES (
        #{{id}},
        #{{name}},
        #{{timeOfStart, typeHandler = service.dataprocessor.dal.LocalDateTimeTypeHandler}},
        #{{fileName}})
      </xsql>
  }

  def bind: Seq[Statement] = Seq(insertEvent)
}

case class Event(id: Long, name: String, timeOfStart: LocalDateTime, fileName: String)