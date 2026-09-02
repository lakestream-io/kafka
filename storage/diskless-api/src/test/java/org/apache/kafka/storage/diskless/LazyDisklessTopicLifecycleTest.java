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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LazyDisklessTopicLifecycleTest {

    @Test
    void loadsOnFirstCallAndRetriesAfterFailure() throws Exception {
        DisklessTopicLifecycle real = mock(DisklessTopicLifecycle.class);
        when(real.listManagedTopics()).thenReturn(CompletableFuture.completedFuture(List.of()));
        AtomicInteger attempts = new AtomicInteger();
        LazyDisklessTopicLifecycle lazy = new LazyDisklessTopicLifecycle(() -> {
            if (attempts.incrementAndGet() == 1) {
                throw new IllegalStateException("provider unavailable");
            }
            return real;
        });

        ExecutionException first = assertThrows(ExecutionException.class,
            () -> lazy.listManagedTopics().get(10, TimeUnit.SECONDS));
        assertEquals("provider unavailable", first.getCause().getMessage());
        assertEquals(List.of(), lazy.listManagedTopics().get(10, TimeUnit.SECONDS));
        assertEquals(List.of(), lazy.listManagedTopics().get(10, TimeUnit.SECONDS));
        assertEquals(2, attempts.get());
        lazy.close();
        verify(real).close();
    }

    @Test
    void closeBeforeLoadCompletesClosesTheLoadedDelegateAndRejectsThePendingCall() throws Exception {
        DisklessTopicLifecycle real = mock(DisklessTopicLifecycle.class);
        // Stubbed so the pending call would succeed if the post-load closed check were missing.
        when(real.listManagedTopics()).thenReturn(CompletableFuture.completedFuture(List.of()));
        CountDownLatch release = new CountDownLatch(1);
        LazyDisklessTopicLifecycle lazy = new LazyDisklessTopicLifecycle(() -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return real;
        });

        CompletableFuture<List<DisklessTopicLifecycle.ManagedTopic>> pending = lazy.listManagedTopics();
        lazy.close();
        release.countDown();
        ExecutionException failure = assertThrows(
                ExecutionException.class,
                () -> pending.get(10, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, failure.getCause());
        verify(real, timeout(10_000)).close();
        verify(real, never()).listManagedTopics();
    }

    @Test
    void rejectsCallsMadeAfterClose() throws Exception {
        DisklessTopicLifecycle real = mock(DisklessTopicLifecycle.class);
        when(real.listManagedTopics()).thenReturn(CompletableFuture.completedFuture(List.of()));
        LazyDisklessTopicLifecycle lazy = new LazyDisklessTopicLifecycle(() -> real);

        assertEquals(List.of(), lazy.listManagedTopics().get(10, TimeUnit.SECONDS));
        lazy.close();
        verify(real).close();

        ExecutionException failure = assertThrows(
                ExecutionException.class,
                () -> lazy.listManagedTopics().get(10, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, failure.getCause());
        verify(real, times(1)).listManagedTopics();
    }
}
