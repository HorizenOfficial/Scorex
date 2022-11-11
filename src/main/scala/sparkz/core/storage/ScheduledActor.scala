package sparkz.core.storage

import akka.actor.{ActorSystem, Cancellable}
import sparkz.core.storage.ScheduledActor.ScheduledActorConfig

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

/**
  * An abstract class representing an actor that runs a scheduled job
  *
  * @param config - The scheduled job configuration
  * @param ec     - The actor's execution context
  */
abstract class ScheduledActor(private val config: ScheduledActorConfig)(implicit ec: ExecutionContext) {
  private val (initialDelay, scheduleInterval) = ScheduledActorConfig.unapply(config).getOrElse(throw new IllegalArgumentException())
  private val job: Cancellable = ActorSystem("scheduledActor").scheduler.scheduleWithFixedDelay(initialDelay, scheduleInterval) {
    () => scheduledJob()
  }

  /**
    * Method to cancel the current job
    *
    * @return - Whether the job is cancelled successfully
    */
  def stopJob: Boolean = job.cancel()

  /**
    * The custom job the actor should run
    */
  protected def scheduledJob(): Unit
}

object ScheduledActor {

  case class ScheduledActorConfig(initialDelay: FiniteDuration, scheduleInterval: FiniteDuration)

}