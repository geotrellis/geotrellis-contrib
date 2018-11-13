package geotrellis.contrib.vlm.cache

import com.github.blemale.scaffeine.Cache
import scala.collection.concurrent.TrieMap

/** A limited cache wrapper that can be "disable" caching (lazy instance creation) */
case class LazyCache[K, V](getUnderlying: () => Cache[K, V], enabled: Boolean = true) {
  @transient lazy val underlying = getUnderlying()

  def getIfPresent(key: K): Option[V] =
    if(enabled) underlying.getIfPresent(key) else None

  def get(key: K, mappingFunction: K => V): V =
    if(enabled) underlying.get(key, mappingFunction) else mappingFunction(key)

  def getAllPresent(keys: Iterable[K]): Map[K, V] =
    if(enabled) underlying.getAllPresent(keys) else Map()

  def put(key: K, value: V): Unit =
    if(enabled) underlying.put(key, value)

  def putAll(map: Map[K, V]): Unit =
    if(enabled) underlying.putAll(map)

  def invalidate(key: K): Unit =
    if(enabled) underlying.invalidate(key)

  def invalidateAll(keys: Iterable[K]): Unit =
    if(enabled) underlying.invalidateAll(keys)

  def invalidateAll(): Unit =
    if(enabled) underlying.invalidateAll()

  def estimatedSize(): Long =
    if(enabled) underlying.estimatedSize() else 0

  def asMap(): collection.concurrent.Map[K, V] =
    if(enabled) underlying.asMap() else TrieMap()

  def cleanUp(): Unit =
    if(enabled) underlying.cleanUp()
}
