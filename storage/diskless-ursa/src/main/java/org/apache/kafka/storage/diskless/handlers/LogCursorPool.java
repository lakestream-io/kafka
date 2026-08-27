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
package org.apache.kafka.storage.diskless.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;

/**
 * A bounded pool for reusing ephemeral {@link LogCursor} instances against a single {@link Log}.
 *
 * <p>Each acquired cursor is exclusive to the caller until the lease is closed. When the pool is exhausted,
 * callers are queued (FIFO) until a cursor is released.
 *
 * <p>The pool intentionally uses a fixed set of cursor names to avoid unbounded cursor creation.
 */
final class LogCursorPool implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LogCursorPool.class);

    private static final IllegalStateException POOL_CLOSED_EXCEPTION =
            new IllegalStateException("Log cursor pool is closed");

    private final Log logInstance;
    private final String cursorNamePrefix;
    private final int maxSize;
    private final ArrayDeque<Slot> idle = new ArrayDeque<>();
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();

    private boolean closed = false;

    LogCursorPool(Log logInstance, String cursorNamePrefix, int maxSize) {
        if (logInstance == null) {
            throw new IllegalArgumentException("log must not be null");
        }
        if (cursorNamePrefix == null || cursorNamePrefix.isBlank()) {
            throw new IllegalArgumentException("cursorNamePrefix must not be null or blank");
        }
        if (maxSize <= 0) {
            throw new IllegalArgumentException("maxSize must be > 0");
        }
        this.logInstance = logInstance;
        this.cursorNamePrefix = cursorNamePrefix;
        this.maxSize = maxSize;
        for (int i = 0; i < maxSize; i++) {
            idle.add(new Slot(i));
        }
    }

    CompletableFuture<Lease> acquire(long startOffset) {
        Slot slot;
        synchronized (this) {
            if (closed) {
                return CompletableFuture.failedFuture(POOL_CLOSED_EXCEPTION);
            }

            slot = idle.poll();
            if (slot == null) {
                Waiter waiter = new Waiter(startOffset);
                waiters.add(waiter);
                return waiter.future;
            }
        }

        CompletableFuture<Lease> result = new CompletableFuture<>();
        prepareLease(slot, startOffset)
                .whenComplete((lease, error) -> {
                    if (error != null) {
                        releaseSlot(slot);
                        result.completeExceptionally(error);
                    } else if (!result.complete(lease)) {
                        lease.close();
                    }
                });
        return result;
    }

    @Override
    public void close() {
        List<Waiter> toFail;
        List<LogCursor> toClose;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            toFail = new ArrayList<>(waiters);
            waiters.clear();
            toClose = new ArrayList<>(idle.size());
            for (Slot slot : idle) {
                if (slot.cursor != null) {
                    toClose.add(slot.cursor);
                }
                slot.cursor = null;
            }
            idle.clear();
        }

        for (Waiter waiter : toFail) {
            waiter.future.completeExceptionally(POOL_CLOSED_EXCEPTION);
        }

        for (LogCursor cursor : toClose) {
            closeCursorQuietly(cursor);
        }
    }

    private CompletableFuture<Lease> prepareLease(Slot slot, long startOffset) {
        LogCursor cursor = slot.cursor;
        if (cursor == null) {
            CompletableFuture<LogCursor> openFuture;
            try {
                openFuture = logInstance.openEphemeralCursor(cursorName(slot.index), startOffset);
                if (openFuture == null) {
                    throw new IllegalStateException("Log.openEphemeralCursor returned null future");
                }
            } catch (Throwable error) {
                return CompletableFuture.failedFuture(error);
            }
            return openFuture
                    .thenApply(newCursor -> {
                        if (newCursor == null) {
                            throw new IllegalStateException("Log.openEphemeralCursor returned null cursor");
                        }
                        slot.cursor = newCursor;
                        return finishPreparedLease(slot);
                    });
        }

        try {
            if (cursor.readOffset() == startOffset) {
                return CompletableFuture.completedFuture(finishPreparedLease(slot));
            }
        } catch (Throwable error) {
            log.debug("Failed to inspect pooled cursor position for log {}; seeking instead",
                    logInstance.id(), error);
        }

        CompletableFuture<Void> seekFuture;
        try {
            seekFuture = cursor.seek(startOffset);
            if (seekFuture == null) {
                throw new IllegalStateException("LogCursor.seek returned null future");
            }
        } catch (Throwable error) {
            return CompletableFuture.failedFuture(error);
        }
        return seekFuture.thenApply(ignored -> finishPreparedLease(slot));
    }

    private Lease finishPreparedLease(Slot slot) {
        synchronized (this) {
            if (!closed) {
                return new Lease(this, slot);
            }
        }

        closeSlotCursor(slot);
        throw POOL_CLOSED_EXCEPTION;
    }

    private void release(Lease lease) {
        if (!lease.released.compareAndSet(false, true)) {
            return;
        }
        releaseSlot(lease.slot);
    }

    private void releaseSlot(Slot slot) {
        if (slot == null) {
            return;
        }

        Waiter waiter = null;
        LogCursor cursorToClose = null;
        synchronized (this) {
            if (closed) {
                cursorToClose = slot.cursor;
                slot.cursor = null;
            } else {
                waiter = pollNextWaiterLocked();
                if (waiter == null) {
                    idle.add(slot);
                    return;
                }
            }
        }

        if (cursorToClose != null) {
            closeCursorQuietly(cursorToClose);
            return;
        }
        if (waiter == null) {
            return;
        }

        Waiter nextWaiter = waiter;
        prepareLease(slot, waiter.startOffset)
                .whenComplete((lease, error) -> {
                    if (error != null) {
                        nextWaiter.future.completeExceptionally(error);
                        releaseSlot(slot);
                    } else if (!nextWaiter.future.complete(lease)) {
                        lease.close();
                    }
                });
    }

    private void closeSlotCursor(Slot slot) {
        LogCursor cursor;
        synchronized (this) {
            cursor = slot.cursor;
            slot.cursor = null;
        }
        if (cursor != null) {
            closeCursorQuietly(cursor);
        }
    }

    private void closeCursorQuietly(LogCursor cursor) {
        try {
            cursor.close();
        } catch (Exception e) {
            log.warn("Failed to close pooled cursor for log {}", logInstance.id(), e);
        }
    }

    private Waiter pollNextWaiterLocked() {
        while (true) {
            Waiter waiter = waiters.poll();
            if (waiter == null) {
                return null;
            }
            if (!waiter.future.isDone()) {
                return waiter;
            }
        }
    }

    private String cursorName(int slotIndex) {
        return cursorNamePrefix + "-" + slotIndex;
    }

    private static final class Slot {
        private final int index;
        private LogCursor cursor;

        private Slot(int index) {
            this.index = index;
        }
    }

    private static final class Waiter {
        private final long startOffset;
        private final CompletableFuture<Lease> future = new CompletableFuture<>();

        private Waiter(long startOffset) {
            this.startOffset = startOffset;
        }
    }

    static final class Lease implements AutoCloseable {
        private final LogCursorPool pool;
        private final Slot slot;
        private final AtomicBoolean released = new AtomicBoolean(false);

        private Lease(LogCursorPool pool, Slot slot) {
            this.pool = pool;
            this.slot = slot;
        }

        LogCursor cursor() {
            return slot.cursor;
        }

        @Override
        public void close() {
            pool.release(this);
        }
    }
}
