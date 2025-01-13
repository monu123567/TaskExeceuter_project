package com.alight.mobile.service.helper;

import java.util.concurrent.Future;

public interface TaskExecutor {

	<T> Future<T> submitTask(Task<T> task);
}
