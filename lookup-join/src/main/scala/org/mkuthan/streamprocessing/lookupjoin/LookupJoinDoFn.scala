package org.mkuthan.streamprocessing.lookupjoin

import scala.annotation.unused
import scala.jdk.CollectionConverters._

import com.spotify.scio.ScioMetrics
import com.spotify.scio.coders.Coder
import com.spotify.scio.coders.CoderMaterializer
import com.typesafe.scalalogging.LazyLogging
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.state.BagState
import org.apache.beam.sdk.state.CombiningState
import org.apache.beam.sdk.state.StateSpecs
import org.apache.beam.sdk.state.TimeDomain
import org.apache.beam.sdk.state.Timer
import org.apache.beam.sdk.state.TimerSpecs
import org.apache.beam.sdk.state.ValueState
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.AlwaysFetched
import org.apache.beam.sdk.transforms.DoFn.Element
import org.apache.beam.sdk.transforms.DoFn.OnTimer
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.DoFn.StateId
import org.apache.beam.sdk.transforms.DoFn.TimerId
import org.apache.beam.sdk.transforms.DoFn.Timestamp
import org.apache.beam.sdk.transforms.windowing.GlobalWindow
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration
import org.joda.time.Instant

object LookupJoinDoFn {
  type InputType[K, V, Lookup] = KV[K, (Iterable[V], Iterable[Lookup])]
  type OutputType[K, V, Lookup] = KV[K, (V, Option[Lookup])]

  type LookupJoinDoFnType[K, V, Lookup] = DoFn[InputType[K, V, Lookup], OutputType[K, V, Lookup]]

  type MaxSeenTimestampStateType = CombiningState[Instant, Instant, Instant]

  private final val MaxAllowedTimestamp: Instant = GlobalWindow.INSTANCE.maxTimestamp()

  final val KeyCacheKey = "KeyCache"
  final val ValuesCacheKey = "ValuesCache"
  final val LookupCacheKey = "LookupJoin"

  final val MaxSeenTimestampKey = "MaxSeenTimestamp"
  final val ValuesReleaseTimestampKey = "ValuesReleaseTimestamp"

  final val ReleaseValuesTimerKey = "ValuesReleaseTimer"
  final val ClearLookupTimerKey = "ClearLookupTimer"

  // TODO: separate metrics for each DoFn instance
  private final val MetricsNamespace = "org.mkuthan.streamprocessing.lookupjoin"

  private final val InvalidTimestampCounter = ScioMetrics.counter(MetricsNamespace, "invalid_timestamp")
  private final val EmptyElementCounter = ScioMetrics.counter(MetricsNamespace, "empty_element")
  private final val LookupCacheSize = Metrics.counter(MetricsNamespace, "lookup_cache_size")
  private final val KeyCacheSize = Metrics.counter(MetricsNamespace, "key_cache_size")
  private final val ValuesCacheSize = Metrics.counter(MetricsNamespace, "values_cache_size")

  private def isLookupExist[Lookup](lookupCacheState: ValueState[Lookup]): Boolean =
    lookupCacheState.read() != null

  private def clearLookup[Lookup](LookupJoinState: ValueState[Lookup]): Unit = {
    if (LookupJoinState.read() != null) LookupCacheSize.dec() // TODO: why guard here?
    LookupJoinState.clear()
  }

  private def updateMaxTimestampSeen(timestamp: Instant, maxTimestampSeenState: MaxSeenTimestampStateType): Instant = {
    maxTimestampSeenState.add(timestamp)
    maxTimestampSeenState.read()
  }

  private def updateCache[K, V, Lookup](
      key: K,
      keyCacheState: ValueState[K],
      values: Iterable[V],
      valuesCacheState: BagState[V],
      lookups: Iterable[Lookup],
      lookupCacheState: ValueState[Lookup]
  ): Unit = {
    if (keyCacheState.read() != null) KeyCacheSize.inc() // TODO: why guard here?
    keyCacheState.write(key)

    ValuesCacheSize.inc(values.size.toLong)
    values.foreach(value => valuesCacheState.add(value))

    lookups.headOption.foreach { lookup => // TODO: why first element?
      if (lookupCacheState.read() != null) LookupCacheSize.inc() // TODO: why guard here?
      lookupCacheState.write(lookup)
    }
  }

  private def emitCachedOutput[K, V, Lookup](
      timestamp: Instant,
      keyCacheState: ValueState[K],
      valuesCacheState: BagState[V],
      lookupCacheState: ValueState[Lookup],
      output: OutputReceiver[OutputType[K, V, Lookup]]
  ): Unit = {
    val key = keyCacheState.read() // TODO: handle empty key
    val lookup = Option(lookupCacheState.read())
    val values = valuesCacheState.read().asScala
    values.foreach { value =>
      output.outputWithTimestamp(KV.of(key, (value, lookup)), timestamp)
    }

    keyCacheState.clear()
    KeyCacheSize.dec()

    valuesCacheState.clear()
    ValuesCacheSize.dec(values.size.toLong)
  }
}

class LookupJoinDoFn[K, V, Lookup](valuesTimeToLive: Duration, lookupTimeToLive: Duration)(
    implicit
    keyCoder: Coder[K],
    leftCoder: Coder[V],
    rightCoder: Coder[Lookup]
) extends LookupJoinDoFn.LookupJoinDoFnType[K, V, Lookup]
    with LazyLogging {

  require(valuesTimeToLive.isLongerThan(Duration.ZERO))
  require(lookupTimeToLive.isLongerThan(Duration.ZERO))

  import LookupJoinDoFn._

  @unused
  @StateId(KeyCacheKey) private val keyCacheSpec = StateSpecs.value[K](CoderMaterializer.beamWithDefault(Coder[K]))

  @unused
  @StateId(ValuesCacheKey) private val valuesCacheSpec = StateSpecs.bag[V](CoderMaterializer.beamWithDefault(Coder[V]))

  @unused
  @StateId(LookupCacheKey) private val lookupCacheSpec =
    StateSpecs.value[Lookup](CoderMaterializer.beamWithDefault(Coder[Lookup]))

  @unused
  @StateId(MaxSeenTimestampKey) private val maxSeenTimestampSpec = StateSpecs.combining(new MaxInstantFn())

  @unused
  @StateId(ValuesReleaseTimestampKey) private val valuesReleaseTimestampSpec = StateSpecs.value[Instant]()

  @unused
  @TimerId(ReleaseValuesTimerKey) private val releaseValuesTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME)

  @unused
  @TimerId(ClearLookupTimerKey) private val clearLookupTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME)

  @unused
  @ProcessElement
  def processElement(
      @Timestamp timestamp: Instant,
      @Element element: InputType[K, V, Lookup],
      @AlwaysFetched @StateId(KeyCacheKey) keyCacheState: ValueState[K],
      // not pre-fetched as it is not always read and may be sizeable
      @StateId(ValuesCacheKey) valuesCacheState: BagState[V],
      @AlwaysFetched @StateId(LookupCacheKey) lookupCacheState: ValueState[Lookup],
      @AlwaysFetched @StateId(MaxSeenTimestampKey) maxSeenTimestampState: MaxSeenTimestampStateType,
      @AlwaysFetched @StateId(ValuesReleaseTimestampKey) valuesReleaseTimestampState: ValueState[Instant],
      @TimerId(ReleaseValuesTimerKey) releaseValuesTimer: Timer,
      @TimerId(ClearLookupTimerKey) clearLookupTimer: Timer,
      output: OutputReceiver[OutputType[K, V, Lookup]]
  ): Unit = {
    val key = element.getKey
    val (values, lookups) = element.getValue

    if (!timestamp.isBefore(MaxAllowedTimestamp)) {
      InvalidTimestampCounter.inc()
    } else if (values.isEmpty && lookups.isEmpty) {
      EmptyElementCounter.inc()
    } else {
      val maxTimestampSeen = updateMaxTimestampSeen(timestamp, maxSeenTimestampState)
      updateCache(key, keyCacheState, values, valuesCacheState, lookups, lookupCacheState)

      if (isLookupExist(lookupCacheState)) {
        emitCachedOutput(timestamp, keyCacheState, valuesCacheState, lookupCacheState, output)
      } else {
        updateReleaseValuesTimer(
          maxTimestampSeen,
          releaseValuesTimer,
          valuesTimeToLive,
          valuesReleaseTimestampState
        )
      }

      updateClearLookupTimer(maxTimestampSeen, clearLookupTimer, lookupTimeToLive)
    }
  }

  @unused
  @OnTimer(ReleaseValuesTimerKey)
  def onValuesReleaseTimer(
      @Timestamp timestamp: Instant,
      @AlwaysFetched @StateId(KeyCacheKey) keyCacheState: ValueState[K],
      @StateId(ValuesCacheKey) valuesCacheState: BagState[V],
      @AlwaysFetched @StateId(LookupCacheKey) lookupCacheState: ValueState[Lookup],
      @StateId(ValuesReleaseTimestampKey) nextValuesReleaseState: ValueState[Instant],
      output: OutputReceiver[OutputType[K, V, Lookup]]
  ): Unit = {
    emitCachedOutput(timestamp, keyCacheState, valuesCacheState, lookupCacheState, output)
    nextValuesReleaseState.clear()
  }

  @unused
  @OnTimer(ClearLookupTimerKey)
  def onClearLookupTimer(
      @AlwaysFetched @StateId(LookupCacheKey) lookupCacheState: ValueState[Lookup]
  ): Unit =
    clearLookup(lookupCacheState)

  private def updateReleaseValuesTimer(
      maxTimestampSeen: Instant,
      timer: Timer,
      timeout: Duration,
      nextValuesReleaseState: ValueState[Instant]
  ): Unit = {
    val maybeTriggerTimestamp = Option(nextValuesReleaseState.read())
    val triggerTimestamp = maybeTriggerTimestamp.getOrElse(
      nextTriggerTimestamp(maxTimestampSeen, timeout)
    )
    nextValuesReleaseState.write(triggerTimestamp)

    // schedule release timer on release timestamp with output watermark held at max timestamp seen until the timer fires
    // if max timestamp seen precedes release timestamp (and thus input watermark) do not set output timestamp - it makes no sense
    if (maxTimestampSeen.isAfter(triggerTimestamp)) {
      timer.set(triggerTimestamp)
    } else {
      timer.withOutputTimestamp(maxTimestampSeen).set(triggerTimestamp)
    }
  }

  private def updateClearLookupTimer(
      maxTimestampSeen: Instant,
      clearLookupTimer: Timer,
      lookupTimeToLive: Duration
  ): Unit = {
    val triggerTimestamp = nextTriggerTimestamp(maxTimestampSeen, lookupTimeToLive)

    // watermark will not be held at max timestamp seen because nothing should output at this timer
    clearLookupTimer.set(triggerTimestamp)
  }

  private def nextTriggerTimestamp(
      timestamp: Instant,
      timeToLive: Duration
  ): Instant = {
    val expirationTimestamp = timestamp.plus(timeToLive)

    if (expirationTimestamp.isBefore(MaxAllowedTimestamp)) {
      expirationTimestamp
    } else {
      MaxAllowedTimestamp
    }
  }
}
