/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.storage.diskless;

import org.apache.kafka.common.KafkaException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Lazily creates a controller-side diskless resource away from the metadata publisher thread.
 * Failed initialization is not cached, so the controller reconciler can supervise and retry it.
 */
final class LazyDisklessResource<T extends AutoCloseable> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LazyDisklessResource.class);

    private final String description;
    private final Supplier<T> loader;
    private final ExecutorService executor;

    private CompletableFuture<T> initialization;
    private T delegate;
    private boolean closeRequested;
    private boolean closeCompleted;

    LazyDisklessResource(String description, Supplier<T> loader) {
        this.description = Objects.requireNonNull(description, "description must not be null");
        this.loader = Objects.requireNonNull(loader, "loader must not be null");
        this.executor = Executors.newSingleThreadExecutor(task -> {
            Thread thread = new Thread(task, "lazy-" + description.replace(' ', '-') + "-loader");
            thread.setDaemon(true);
            return thread;
        });
    }

    <R> CompletableFuture<R> call(Function<T, CompletableFuture<R>> operation) {
        Objects.requireNonNull(operation, "operation must not be null");
        CompletableFuture<T> currentInitialization = initializationForCall();
        if (currentInitialization == null) {
            return CompletableFuture.failedFuture(
                    new CancellationException(description + " is closed"));
        }

        CompletableFuture<R> result = new CompletableFuture<>();
        AtomicReference<CompletableFuture<R>> source = new AtomicReference<>();
        propagateCancellation(result, source);
        currentInitialization.whenCompleteAsync(
                (initialized, initializationError) -> completeCall(
                        operation,
                        result,
                        source,
                        initialized,
                        initializationError),
                executor);
        return result;
    }

    private synchronized CompletableFuture<T> initializationForCall() {
        return closeRequested ? null : ensureInitialization();
    }

    private static <R> void propagateCancellation(
            CompletableFuture<R> result,
            AtomicReference<CompletableFuture<R>> source) {
        result.whenComplete((ignored, error) -> {
            CompletableFuture<R> currentSource = source.get();
            if (result.isCancelled() && currentSource != null) {
                currentSource.cancel(true);
            }
        });
    }

    private <R> void completeCall(
            Function<T, CompletableFuture<R>> operation,
            CompletableFuture<R> result,
            AtomicReference<CompletableFuture<R>> source,
            T initialized,
            Throwable initializationError) {
        if (result.isDone()) {
            return;
        }
        if (initializationError != null) {
            result.completeExceptionally(unwrap(initializationError));
            return;
        }
        synchronized (this) {
            if (closeRequested) {
                result.completeExceptionally(
                        new CancellationException(description + " is closed"));
                return;
            }
        }
        CompletableFuture<R> operationSource = invokeOperation(operation, initialized, result);
        if (operationSource == null) {
            return;
        }
        source.set(operationSource);
        if (result.isCancelled()) {
            operationSource.cancel(true);
            return;
        }
        operationSource.whenComplete((value, error) -> completeResult(result, value, error));
    }

    private <R> CompletableFuture<R> invokeOperation(
            Function<T, CompletableFuture<R>> operation,
            T initialized,
            CompletableFuture<R> result) {
        try {
            return Objects.requireNonNull(
                    operation.apply(initialized),
                    description + " operation returned null future");
        } catch (Throwable error) {
            result.completeExceptionally(error);
            return null;
        }
    }

    private static <R> void completeResult(
            CompletableFuture<R> result,
            R value,
            Throwable error) {
        if (error == null) {
            result.complete(value);
        } else {
            result.completeExceptionally(error);
        }
    }

    private CompletableFuture<T> ensureInitialization() {
        if (initialization != null) {
            return initialization;
        }
        CompletableFuture<T> started = CompletableFuture.supplyAsync(() -> {
            try {
                return Objects.requireNonNull(loader.get(), description + " loader returned null");
            } catch (RuntimeException | Error error) {
                throw error;
            } catch (Throwable error) {
                throw new CompletionException(error);
            }
        }, executor);
        initialization = started;
        started.whenComplete((initialized, error) -> completeInitialization(started, initialized, error));
        return started;
    }

    private void completeInitialization(
            CompletableFuture<T> started,
            T initialized,
            Throwable error) {
        boolean closeAfterInitialization;
        synchronized (this) {
            if (initialization != started) {
                return;
            }
            if (error == null) {
                delegate = initialized;
            } else {
                initialization = null;
            }
            closeAfterInitialization = closeRequested;
        }
        if (closeAfterInitialization) {
            executor.execute(() -> {
                if (initialized != null) {
                    try {
                        closeDelegate(initialized);
                    } catch (Exception closeError) {
                        log.warn(
                                "Failed to close {} after its lazy initialization completed",
                                description,
                                closeError);
                    }
                } else {
                    finishClose();
                }
            });
        }
    }

    @Override
    public void close() throws Exception {
        T initialized;
        CompletableFuture<T> currentInitialization;
        synchronized (this) {
            if (closeCompleted) {
                return;
            }
            closeRequested = true;
            initialized = delegate;
            currentInitialization = initialization;
            if (initialized == null && currentInitialization == null) {
                finishCloseLocked();
                return;
            }
        }
        if (initialized != null) {
            closeDelegate(initialized);
        }
        // An in-progress loader is deliberately not blocked or abandoned here. Its completion
        // callback closes any resource it managed to construct and then shuts down the executor.
    }

    private void closeDelegate(T initialized) throws Exception {
        try {
            initialized.close();
        } catch (Exception error) {
            throw error;
        } catch (Throwable error) {
            throw new KafkaException("Failed to close " + description, error);
        }
        finishClose();
    }

    private void finishClose() {
        synchronized (this) {
            finishCloseLocked();
        }
    }

    private void finishCloseLocked() {
        closeCompleted = true;
        delegate = null;
        executor.shutdown();
    }

    private static Throwable unwrap(Throwable error) {
        Throwable current = error;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
                && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }
}
