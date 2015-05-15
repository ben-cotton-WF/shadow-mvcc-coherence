# Introduction #

Most APIs and configuration options work just the same. In particular you can execute `EntryProcessor`s against caches exactly as for normal caches (but see below about `processAll()`). shadow MVCC wraps them up so that when you get the current cache entry you see the previous version value, and when you set the entry value it will create the new one with the current transaction timestamp.

# APIs not implemented #

## Locking ##

You can't lock and unlock cache entries, but then part of the point of MVCC is that you don't need to.

## Expiry and Eviction ##

`put()` with an expiry is not implemented. If you configure expiry or eviction in the cache configuration, the results are unpredictable and almost certainly not what you want. About the nearest thing is the snapshot capability that allows you to remove old versions. For MVCC to work, we must clear cache entries in a way that provides a consistent view, which is not possible with default expiry and eviction behaviour.

## CacheLoader ##

Read-through is tricky. Conceptually, what would if mean to read through in an MVCC cache, what should the timestamp of the read entry be? What if the entry was there but had been deleted (i.e. there is a deleted version present)? What if the entry is absent at the timestamp of the reading transaction but is present with a leter timestamp? Technically, cache gets are actually implemented as filter queries under the covers so the Coherence read through mechanism won't be invoked even if configured.

# Implemented with caveats #

## EntryProcessor.processAll() ##

`processAll` will never be called. This is because the MVCC wrapper must perform some preparation serialised on the key cache entry: internally invocation is against the key cache with partition-local transactions to modify the version cache - essential to avoid race conditions and deadlocks. As a workaround, there is a `Reducer` interface you can implement to post-process the map of results in the storage node as an alternative mechanism of modifying maps. Also there is a special`NoReturn` marker class that can be returned from the `process` method to indicate that this entry should be excluded from the returned map.

## CacheStore, Trigger, BackingMapListener ##

You could set these on the version cache, but they will be called in the context of the physical model, so:
  * the key will be a `VersionedKey` including logical key and transaction id
  * almost everything will be an insert, even deletes, which can be identified by a decoration
  * updates always signify commit - again, there's a decoration that tells you that an entry is committed
  * deleted signify either a rollback, or old versions being deleted for a snapshot.

## MapListener ##

MapListeners are wrapped up to behave pretty much like standard MapListeners, but you can choose whether to receive uncommitted events and out-of-order events. Default is committed, and only in-order (out of sequence updates are not reported). This is so that CQCs will work correctly (yet to be tested).
