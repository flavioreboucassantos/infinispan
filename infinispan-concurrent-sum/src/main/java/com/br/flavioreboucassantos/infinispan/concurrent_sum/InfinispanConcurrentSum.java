package com.br.flavioreboucassantos.infinispan.concurrent_sum;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.TransactionMode;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;

public class InfinispanConcurrentSum {

	/*
	 * Change Here to Select Between runThreadSafeSumTask or runNonThreadSafeSumTask
	 */
	static public final boolean isThreadSafe = false;

	/*
	 * Configurations of Concurrent Sum
	 */
	static private final String cacheName = "concurrent-sum";
	static private final String keyName = "valor";

	static private final int numberOfAdders = 5;
	static private final int numberOfSums = 500;
	static private final long nsTimeBetweenSums = 1000000 * 1; // 1000000 is 1 ms & * 1000 is 1 sec

	static private final AtomicInteger countAddersConcluded = new AtomicInteger(0);
	static private final AtomicInteger countRollbacks = new AtomicInteger(0);

	static private final RemoteCacheManager remoteCacheManager;
	static private final RemoteCache<String, Integer> cache;

	static {
		ConfigurationBuilder cb = new ConfigurationBuilder();

		cb.addServer()
				.host("127.0.0.1")
				.port(11222)
				.security().authentication()
				.username("admin")
				.password("admin");

		cb.remoteCache(cacheName)
				.transactionManagerLookup(RemoteTransactionManagerLookup.getInstance())
				.transactionMode(TransactionMode.NON_XA);

		cb.transactionTimeout(60, TimeUnit.SECONDS);

		remoteCacheManager = new RemoteCacheManager(cb.build());

		cache = remoteCacheManager.getCache(cacheName);
	}

	static private void print(Object p) {
		System.out.println(p);
	}

	static private boolean allAddersConcluded() {
		return (countAddersConcluded.get() < numberOfAdders) ? false : true;
	}

	static public void main(String[] args) throws InterruptedException {
//		Cache capabilities: Transactional
//		Transaction mode: NON_XA - Infinispan disables transaction recovery and enlists the cache with the Java transaction manager through the XAResource API. Disabling recovery means that you cannot recover failed transactions but do not have the performance overhead of handling in-doubt transactions.
//		Locking mode: Optimistic - Infinispan locks keys when it invokes the commit() method. Keys are locked for shorter periods of time which reduces overall latency but makes transaction recovery less efficient.

// 		Storage Type: OFF_HEAP - Off-heap storage uses less memory per entry compared with JVM heap storage and can improve performance by avoiding avoids garbage collection (GC) runs.
//		Concurrency level: 32 - Configures the number of locks to create in the shared pool for lock striping.
//		Lock timeout: 1500 ms - Sets how many milliseconds to wait for lock acquisition.
//		Lock striping: true - Uses a shared pool of locks for all entries in the cache. Striping lowers the memory footprint for locks but can reduce concurrency. If you disable striping, a lock is created for each entry in the cache.

//		Read isolation levels guarantee whether or not data in the cache has changed during a transaction.
// 		Read isolation level: REPEATABLE_READ - Read operations return the same value that Infinispan initially retrieves for an entry during a transaction. This is the default read isolation level because it guarantees consistency.

//		Stop timeout: 30000ms
//		Complete timeout: 60000ms
//		Reaper interval: 30000ms

//		Cache configuration: concurrent-sum.json

		cache.put(keyName, 0);

		for (int i = 0; i < numberOfAdders; i++) {
			Thread thread = new Thread(new InfinispanRunnableConcurrentSum(cache, keyName, numberOfSums, nsTimeBetweenSums));
			thread.start();
		}

		final long msTimeOfStart = System.currentTimeMillis();

		while (!allAddersConcluded())
			Thread.yield();

		final long msTimeToFinish = System.currentTimeMillis() - msTimeOfStart;

		final int valorEsperado = numberOfAdders * numberOfSums;
		final int valorEncontrado = cache.get(keyName);

		print("Tempo gasto: " + msTimeToFinish + " milliseconds");
		print("Valor esperado: " + valorEsperado);
		print("Valor encontrado: " + valorEncontrado);
		print("Rollbacks realizados: " + countRollbacks.get());

		remoteCacheManager.close();
	}

	static public void adderConcluded(final int _countRollbacks) {
		countRollbacks.addAndGet(_countRollbacks);
		countAddersConcluded.incrementAndGet();
	}
}
