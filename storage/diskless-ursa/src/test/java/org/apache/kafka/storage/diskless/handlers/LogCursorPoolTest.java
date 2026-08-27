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

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.lakestream.api.Log;
import io.lakestream.api.LogCursor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LogCursorPoolTest {

    @Test
    void testAcquireReusesCursorAndSkipsSeekAtCurrentOffset() throws Exception {
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);

        long startOffset = 7L;
        when(logInstance.openEphemeralCursor(anyString(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(startOffset);
        when(cursor.seek(anyLong())).thenReturn(CompletableFuture.completedFuture(null));

        LogCursorPool pool = new LogCursorPool(logInstance, "test-cursor", 1);

        try (LogCursorPool.Lease lease = pool.acquire(startOffset).get()) {
            assertSame(cursor, lease.cursor());
        }

        try (LogCursorPool.Lease lease = pool.acquire(startOffset).get()) {
            assertSame(cursor, lease.cursor());
        }

        verify(logInstance, times(1)).openEphemeralCursor("test-cursor-0", startOffset);
        verify(cursor, never()).seek(anyLong());
    }

    @Test
    void testExhaustedPoolQueuesWaitersInOrderAndSeeksReusedCursor() throws Exception {
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(logInstance.openEphemeralCursor(anyString(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(1L, 2L);
        when(cursor.seek(anyLong())).thenReturn(CompletableFuture.completedFuture(null));

        LogCursorPool pool = new LogCursorPool(logInstance, "test-cursor", 1);
        LogCursorPool.Lease firstLease = pool.acquire(1L).get();
        CompletableFuture<LogCursorPool.Lease> second = pool.acquire(2L);
        CompletableFuture<LogCursorPool.Lease> third = pool.acquire(3L);

        assertFalse(second.isDone());
        assertFalse(third.isDone());

        firstLease.close();
        LogCursorPool.Lease secondLease = second.get();
        assertSame(cursor, secondLease.cursor());
        assertFalse(third.isDone());

        secondLease.close();
        try (LogCursorPool.Lease thirdLease = third.get()) {
            assertSame(cursor, thirdLease.cursor());
        }

        var ordered = inOrder(cursor);
        ordered.verify(cursor).seek(2L);
        ordered.verify(cursor).seek(3L);
        verify(logInstance, times(1)).openEphemeralCursor(anyString(), anyLong());
    }

    @Test
    void testCloseFailsWaitersAndClosesCursorWhenActiveLeaseReturns() throws Exception {
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        when(logInstance.openEphemeralCursor(anyString(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(cursor));

        LogCursorPool pool = new LogCursorPool(logInstance, "test-cursor", 1);
        LogCursorPool.Lease activeLease = pool.acquire(1L).get();
        CompletableFuture<LogCursorPool.Lease> waiter = pool.acquire(2L);

        pool.close();

        ExecutionException waiterError = assertThrows(ExecutionException.class, waiter::get);
        assertTrue(waiterError.getCause() instanceof IllegalStateException);
        verify(cursor, never()).close();

        activeLease.close();
        verify(cursor).close();

        ExecutionException closedError = assertThrows(ExecutionException.class, () -> pool.acquire(3L).get());
        assertTrue(closedError.getCause() instanceof IllegalStateException);
    }

    @Test
    void testCloseWhileCursorOpenIsPendingClosesLateCursorAndFailsAcquire() throws Exception {
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        CompletableFuture<LogCursor> openFuture = new CompletableFuture<>();
        when(logInstance.openEphemeralCursor(anyString(), anyLong())).thenReturn(openFuture);

        LogCursorPool pool = new LogCursorPool(logInstance, "test-cursor", 1);
        CompletableFuture<LogCursorPool.Lease> acquireFuture = pool.acquire(1L);

        pool.close();
        openFuture.complete(cursor);

        ExecutionException acquireError = assertThrows(ExecutionException.class, acquireFuture::get);
        assertTrue(acquireError.getCause() instanceof IllegalStateException);
        verify(cursor).close();
    }

    @Test
    void testCloseWhileCursorSeekIsPendingClosesCursorAndFailsAcquire() throws Exception {
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        CompletableFuture<Void> seekFuture = new CompletableFuture<>();
        when(logInstance.openEphemeralCursor(anyString(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(1L);
        when(cursor.seek(2L)).thenReturn(seekFuture);

        LogCursorPool pool = new LogCursorPool(logInstance, "test-cursor", 1);
        pool.acquire(1L).get().close();
        CompletableFuture<LogCursorPool.Lease> acquireFuture = pool.acquire(2L);

        pool.close();
        seekFuture.complete(null);

        ExecutionException acquireError = assertThrows(ExecutionException.class, acquireFuture::get);
        assertTrue(acquireError.getCause() instanceof IllegalStateException);
        verify(cursor).close();
    }

    @Test
    void testCancelWhileCursorOpenIsPendingReturnsSlotToPool() throws Exception {
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        CompletableFuture<LogCursor> openFuture = new CompletableFuture<>();
        when(logInstance.openEphemeralCursor(anyString(), anyLong())).thenReturn(openFuture);
        when(cursor.readOffset()).thenReturn(1L);

        LogCursorPool pool = new LogCursorPool(logInstance, "test-cursor", 1);
        CompletableFuture<LogCursorPool.Lease> canceledAcquire = pool.acquire(1L);

        assertTrue(canceledAcquire.cancel(false));
        openFuture.complete(cursor);

        try (LogCursorPool.Lease lease = pool.acquire(1L).get()) {
            assertSame(cursor, lease.cursor());
        }
        verify(logInstance, times(1)).openEphemeralCursor(anyString(), anyLong());

        pool.close();
        verify(cursor).close();
    }

    @Test
    void testCancelWhileCursorSeekIsPendingReturnsSlotToPool() throws Exception {
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        CompletableFuture<Void> seekFuture = new CompletableFuture<>();
        when(logInstance.openEphemeralCursor(anyString(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(cursor));
        when(cursor.readOffset()).thenReturn(1L, 2L);
        when(cursor.seek(2L)).thenReturn(seekFuture);

        LogCursorPool pool = new LogCursorPool(logInstance, "test-cursor", 1);
        pool.acquire(1L).get().close();
        CompletableFuture<LogCursorPool.Lease> canceledAcquire = pool.acquire(2L);

        assertTrue(canceledAcquire.cancel(false));
        seekFuture.complete(null);

        try (LogCursorPool.Lease lease = pool.acquire(2L).get()) {
            assertSame(cursor, lease.cursor());
        }
        verify(logInstance, times(1)).openEphemeralCursor(anyString(), anyLong());
        verify(cursor, times(1)).seek(2L);

        pool.close();
        verify(cursor).close();
    }

    @Test
    void testFailedOpenReturnsSlotToPool() throws Exception {
        Log logInstance = mock(Log.class);
        LogCursor cursor = mock(LogCursor.class);
        RuntimeException openFailure = new RuntimeException("open failed");
        when(logInstance.openEphemeralCursor(anyString(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(openFailure))
                .thenReturn(CompletableFuture.completedFuture(cursor));

        LogCursorPool pool = new LogCursorPool(logInstance, "test-cursor", 1);

        ExecutionException firstError = assertThrows(ExecutionException.class, () -> pool.acquire(1L).get());
        assertSame(openFailure, firstError.getCause());

        try (LogCursorPool.Lease lease = pool.acquire(2L).get()) {
            assertNotNull(lease.cursor());
        }

        verify(logInstance, times(2)).openEphemeralCursor(anyString(), anyLong());
    }
}
