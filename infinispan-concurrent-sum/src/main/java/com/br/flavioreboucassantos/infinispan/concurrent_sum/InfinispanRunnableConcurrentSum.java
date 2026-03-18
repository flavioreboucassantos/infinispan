package com.br.flavioreboucassantos.infinispan.concurrent_sum;

import org.infinispan.client.hotrod.RemoteCache;

import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;

public class InfinispanRunnableConcurrentSum implements Runnable {

	private final int numberOfSums;
	private final long nsTimeBetweenSums;

	final RemoteCache<String, Integer> cache;
	private final String keyName;

	/*
	 * Non Thread Safe Sum Task
	 */
	private void nonThreadSafeSumTask() throws Exception {
		Integer integer = cache.get(keyName);
		integer++;
		cache.put(keyName, integer);
	}

	private void runNonThreadSafeSumTask() {
		int numberOfSum = 0;
		try {
			// sum every nsTimeBetweenSums
			while (numberOfSum++ < numberOfSums) {
				nonThreadSafeSumTask();
				final long timeNextRun = System.nanoTime() + nsTimeBetweenSums;
//				System.out.println(numberOfSum + " >>> " + timeNextRun);
				while (System.nanoTime() < timeNextRun)
					Thread.yield();
			}
		} catch (final Exception e) {
			e.printStackTrace();
		} finally {
			InfinispanConcurrentSum.adderConcluded(0);
		}
	}

	/*
	 * Thread Safe Sum Task
	 */
	private boolean threadSafeSumTask() throws Exception {
		final TransactionManager tm = cache.getTransactionManager();

		try {
			tm.begin(); // (!) NÃO TRAVA

			Integer integer = cache.get(keyName);
			integer++;
			cache.put(keyName, integer);

			tm.commit(); // O Infinispan verifica versões aqui

			return true;
		} catch (Exception e) {
			try {
				tm.rollback();
			} catch (IllegalStateException | SecurityException | SystemException e1) {
//				e1.printStackTrace();
			}
//			System.err.println("Conflito de concorrência: " + e.getMessage());
//			e.printStackTrace();
			return false;
		}
	}

	private void runThreadSafeSumTask() {
		int numberOfSum = 0;
		int countRollbacks = 0;
		try {
			// sum every nsTimeBetweenSums
			while (numberOfSum++ < numberOfSums) {
				/*
				 * Locking mode: Optimistic && Read isolation level: REPEATABLE_READ Optimistic trava a key quando UMA THREAD consegue commit, mas as outras THREADS já
				 * receberam o valor DIRTY por não haver travado antes de READ. O DIRTY força o ROLLBACK e por isso é preciso retentar threadSafeSumTask()
				 */
				if (!threadSafeSumTask()) {
					numberOfSum--;
					countRollbacks++;
				}
				final long timeNextRun = System.nanoTime() + nsTimeBetweenSums;
//				System.out.println(numberOfSum + " >>> " + timeNextRun);
				while (System.nanoTime() < timeNextRun)
					Thread.yield();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			InfinispanConcurrentSum.adderConcluded(countRollbacks);
		}
	}

	public InfinispanRunnableConcurrentSum(RemoteCache<String, Integer> cache, final String keyName, final int numberOfSums, final long nsTimeBetweenSums) {
		this.numberOfSums = numberOfSums;
		this.nsTimeBetweenSums = nsTimeBetweenSums;

		this.cache = cache;
		this.keyName = keyName;
	}

	@Override
	public void run() {
		if (InfinispanConcurrentSum.isThreadSafe)
			runThreadSafeSumTask();
		else
			runNonThreadSafeSumTask();
	}
}
