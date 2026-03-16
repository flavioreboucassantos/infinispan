package com.br.flavioreboucassantos.concurrent_sum;

import org.infinispan.client.hotrod.RemoteCache;

import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;

public class RunnableConcurrentSum implements Runnable {

	private final int numberOfSums;
	private final long nsTimeBetweenSums;

	final RemoteCache<String, Integer> cache;
	private final String keyName;

	/*
	 * Non Thread Safe Sum Task
	 */
	private void nonThreadSafeSumTask() {
		Integer integer = cache.get(keyName);
		integer++;
		cache.put("valor", integer);
	}

	private void runNonThreadSafeSumTask() {
		// sum every nsTimeBetweenSums
		int numberOfSum = 0;
		while (numberOfSum++ < numberOfSums) {
			nonThreadSafeSumTask();
			final long timeNextRun = System.nanoTime() + nsTimeBetweenSums;
//			System.out.println(numberOfSum + " >>> " + timeNextRun);
			while (System.nanoTime() < timeNextRun)
				Thread.yield();
		}
		ConcurrentSum.adderConcluded(0);
	}

	/*
	 * Thread Safe Sum Task
	 */
	private boolean threadSafeSumTask() {
		final TransactionManager tm = cache.getTransactionManager();

		try {
			tm.begin(); // (!) NÃO TRAVA

			Integer integer = cache.get(keyName);
			integer++;
			cache.put("valor", integer);

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
		// sum every nsTimeBetweenSums
		int numberOfSum = 0;
		int countRollbacks = 0;
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
//			System.out.println(numberOfSum + " >>> " + timeNextRun);
			while (System.nanoTime() < timeNextRun)
				Thread.yield();
		}
		ConcurrentSum.adderConcluded(countRollbacks);
	}

	public RunnableConcurrentSum(RemoteCache<String, Integer> cache, final String keyName, final int numberOfSums, final long nsTimeBetweenSums) {
		this.numberOfSums = numberOfSums;
		this.nsTimeBetweenSums = nsTimeBetweenSums;

		this.cache = cache;
		this.keyName = keyName;
	}

	@Override
	public void run() {
		if (ConcurrentSum.isThreadSafe)
			runThreadSafeSumTask();
		else
			runNonThreadSafeSumTask();
	}
}
