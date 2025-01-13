package com.alight.mobile.service.helper;

import java.util.*;
import java.util.concurrent.*;

public class TaskExecutorService implements TaskExecutor {

	private final ExecutorService executorService;
	private final Map<UUID, Semaphore> groupSemaphores = new ConcurrentHashMap<>();
	private final Queue<Runnable> taskQueue = new ConcurrentLinkedQueue<>();
	private final CompletionService<String> completionService;

	public TaskExecutorService(int maxConcurrency) {
		this.executorService = Executors.newFixedThreadPool(maxConcurrency);
		this.completionService = new ExecutorCompletionService<>(executorService);
	}

	@Override
	public <T> Future<T> submitTask(Task<T> task) {
		CompletableFuture<T> future = new CompletableFuture<>();
		taskQueue.add(() -> executeTask(task, future));
		processQueue();

		return future;
	}

	private <T> void executeTask(Task<T> task, CompletableFuture<T> future) {
		try {
			Semaphore semaphore = groupSemaphores.computeIfAbsent(task.taskGroup().groupUUID(), k -> new Semaphore(1));
			semaphore.acquire();
			try {
				T result = task.taskAction().call();
				future.complete(result);
				completionService.submit(() -> result.toString());
			} catch (Exception e) {
				future.completeExceptionally(e);
			} finally {
				semaphore.release();
				processQueue();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			future.completeExceptionally(e);
		}
	}

	private synchronized void processQueue() {
		Runnable nextTask = taskQueue.poll();
		if (nextTask != null) {
			executorService.submit(nextTask);
		}
	}

	public void shutdown() {
		executorService.shutdown();
	}

	public static void main(String[] args) throws Exception {
		TaskExecutorService executor = new TaskExecutorService(5);

		TaskGroup group1 = new TaskGroup(UUID.randomUUID());
		TaskGroup group2 = new TaskGroup(UUID.randomUUID());
		TaskGroup group3 = new TaskGroup(UUID.randomUUID());

		Task<String> task1 = new Task<>(UUID.randomUUID(), group1, TaskType.READ, () -> {
			Thread.sleep(1000);
			return "Task 1 from Group 1 Completed";
		});

		Task<String> task2 = new Task<>(UUID.randomUUID(), group1, TaskType.READ, () -> {
			Thread.sleep(1000);
			return "Task 2 from Group 1 Completed";
		});

		Task<String> task3 = new Task<>(UUID.randomUUID(), group1, TaskType.READ, () -> {
			Thread.sleep(1000);
			return "Task 3 from Group 1 Completed";
		});

		Task<String> task4 = new Task<>(UUID.randomUUID(), group2, TaskType.WRITE, () -> {
			Thread.sleep(1000);
			return "Task 4 from Group 2 Completed";
		});

		Task<String> task5 = new Task<>(UUID.randomUUID(), group3, TaskType.WRITE, () -> {
			Thread.sleep(1000);
			return "Task 5 from Group 3 Completed";
		});

		executor.submitTask(task1);
		executor.submitTask(task2);
		executor.submitTask(task3);
		executor.submitTask(task4);
		executor.submitTask(task5);

		for (int i = 0; i < 5; i++) {
			Future<String> completedFuture = executor.completionService.take();
			System.out.println(completedFuture.get());
		}

		executor.shutdown();
	}
}
