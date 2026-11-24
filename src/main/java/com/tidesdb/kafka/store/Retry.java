package com.tidesdb.kafka.store;

import com.tidesdb.TidesDBException;

/**
 * Retries a TidesDB call that reports {@link TidesDBException#ERR_LOCKED}.
 *
 * <p>{@code ERR_LOCKED} is transient contention, never a failed operation: the
 * position did not move and nothing was written, and the remedy is always to
 * try again. Any read or iterator step can report it while a compaction moves
 * the sources under it, and a commit can report it while a backup is admitting
 * writers. Every other error is passed through on the first attempt.
 */
final class Retry {

    /** Attempts before giving up, roughly a second of backoff in all. */
    static final int MAX_ATTEMPTS = 20;

    private static final long MAX_BACKOFF_MS = 64;

    @FunctionalInterface
    interface Op<T> {
        T call() throws TidesDBException;
    }

    private Retry() {
    }

    static <T> T run(Op<T> op) throws TidesDBException {
        for (int attempt = 0; ; attempt++) {
            try {
                return op.call();
            } catch (TidesDBException e) {
                if (!e.isRetryable() || attempt + 1 >= MAX_ATTEMPTS) {
                    throw e;
                }
                backoff(attempt);
            }
        }
    }

    private static void backoff(int attempt) throws TidesDBException {
        long ms = Math.min(1L << Math.min(attempt, 6), MAX_BACKOFF_MS);
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new TidesDBException("Interrupted while retrying", TidesDBException.ERR_LOCKED);
        }
    }
}
