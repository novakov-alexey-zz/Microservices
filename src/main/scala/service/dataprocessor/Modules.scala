package service.dataprocessor

import com.google.inject.{AbstractModule, Guice}
import org.mybatis.scala.config.Configuration
import service.dataprocessor.dal.{EventDao, EventDaoImpl, EventDaoMapping}

object Modules {
  def createMybatisConfig(): Configuration = Configuration("mybatis.xml").
    addSpace("service.dataprocessor.dal.Dao") { space ⇒
      space ++= EventDaoMapping
    }

  val persistenceContext = createMybatisConfig().createPersistenceContext
  val injector = Guice.createInjector(new DataProcessorModule)
}

class DataProcessorModule extends AbstractModule {
  override def configure(): Unit = bind(classOf[EventDao]) to classOf[EventDaoImpl]
}
