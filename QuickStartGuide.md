# Configuration #

To use Shadow MVCC you need the shadow MVCC jar on your classpath, as well as coherence.jar version 3.7.1.
Your cache configuration must ensure that related key and version caches are on the same distributed service. This is determined by suffixes to the logical cache name so the easy way is with something like:

```
      <cache-name>mytestcache-*</cache-name>
```

You **must** use POF, and your POF config must include the MVCC POF configuration:

```
  <user-type-list>
  
    <include>mvcc-pof-config.xml</include>
.
.
.
  </user-type-list>
```

Your storage nodes need to use the MVCC cache factory builder to ensure the transaction monitor threads are started, so on your JVM command line:

```
-Dtangosol.coherence.cachefactorybuilder=com.shadowmvcc.coherence.monitor.CacheFactoryBuilder
```

# Code #

You must first create an instance of the MVCC transaction manager, which itself needs a source of timestamps:

```
        TimestampSource timestampSource = new SystemTimestampSource();
        
        transactionManager = new ThreadTransactionManager(
                timestampSource, false, false, readCommitted);

```

Next, get your `NamedCache` instance:

```
        NamedCache cache = transactionManager.getCache("mytestcache");
```

You can do (almost) anything with this cache that you would with a normal Coherence cache. Any cache instance obtained from the same `TransactionManager` will participate in the same transaction. As we are using a `ThreadTransactionManager`, there is a separate transaction associated with each thread, so operations in two threads on the same cache will be in separate transactions.

Now, do some work on the cache:

```
        cache.put(1, "version 1");
```

And commit the changes:

```
        transactionManager.getTransaction().commit();
```