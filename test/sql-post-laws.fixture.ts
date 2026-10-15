import { expect, it, vi } from "vitest";
import type {
  WatchdogExecutor,
  WatchdogQueuedJob,
} from "@fungi.computer/watchdog";
import type { AsyncWatchdogRuntime } from "@fungi.computer/watchdog/d1";
import type { PostEnvelope, PostDeliveryResult } from "../src/post.js";
import type { SqlPostRuntime } from "../src/post-d1.js";

export type Fixture = Readonly<{
  watchdog: AsyncWatchdogRuntime;
  worker: (
    execute: WatchdogExecutor,
    useDatabaseTime?: boolean,
  ) => AsyncWatchdogRuntime;
  commitJob: (job: WatchdogQueuedJob) => Promise<void>;
  lease: (jobId: string) => Promise<number>;
  post: SqlPostRuntime;
  reopen: () => Readonly<{
    watchdog: AsyncWatchdogRuntime;
    post: SqlPostRuntime;
  }>;
  commit: (envelope: PostEnvelope, fail?: boolean) => Promise<void>;
  purge: (key: string, fail?: boolean) => Promise<void>;
  counts: () => Promise<readonly number[]>;
  clock: { now: number };
  deliveries: string[];
  receiver: { run: () => Promise<PostDeliveryResult> };
  close: () => Promise<void>;
}>;
const envelope = (key = "one", text = "hello"): PostEnvelope => ({
  key,
  kind: "account",
  address: { id: "a" },
  event: { text },
});

export function sqlPostLaws(open: () => Promise<Fixture>) {
  it("retains the receiver's parsed receipt with its terminal delivery", async () => {
    const f = await open();
    try {
      f.receiver.run = () =>
        Promise.resolve({
          status: "accepted",
          receipt: { providerMessageId: "provider-1" },
        });
      await f.commit(envelope());
      await f.watchdog.tick();
      expect(await f.post.read("one")).toEqual({
        state: "delivered",
        receipt: { providerMessageId: "provider-1" },
      });
      expect(await f.reopen().post.read("one")).toEqual(
        await f.post.read("one"),
      );
    } finally {
      await f.close();
    }
  });
  it("purges every attempt in selected queues atomically and leaves other queues runnable", async () => {
    const f = await open();
    try {
      await f.commit(envelope("owned"));
      f.receiver.run = () => Promise.reject(new Error("retry"));
      await f.watchdog.tick();
      const pending = await f.post.read("owned");
      if (pending?.state !== "pending")
        throw new Error("Expected pending delivery");
      f.clock.now = pending.nextAttemptAt + 1;
      await f.post.enqueueDue();
      f.receiver.run = () => Promise.resolve("accepted");
      await f.watchdog.tick();
      await f.commit({ ...envelope("other"), address: { id: "other" } });
      expect(await f.counts()).toEqual([2, 2, 3]);
      await expect(f.purge("owned", true)).rejects.toThrow();
      expect(await f.counts()).toEqual([2, 2, 3]);
      await f.purge("owned");
      expect(await f.counts()).toEqual([1, 1, 1]);
      expect(await f.post.read("owned")).toBeUndefined();
      await f.watchdog.tick();
      expect(await f.post.read("other")).toEqual({ state: "delivered" });
    } finally {
      await f.close();
    }
  });

  it("rolls back the source fact, Post envelope and Watchdog job together", async () => {
    const f = await open();
    try {
      await expect(f.commit(envelope(), true)).rejects.toThrow();
      expect(await f.counts()).toEqual([0, 0, 0]);
      await f.commit(envelope());
      // No tick has run: all three durable writes already exist.
      expect(await f.counts()).toEqual([1, 1, 1]);
      await f.watchdog.tick();
      expect(await f.post.read("one")).toEqual({ state: "delivered" });
      expect(f.deliveries).toEqual(["one"]);
    } finally {
      await f.close();
    }
  });
  it("replays identically and rejects conflicting cargo without committing the new fact", async () => {
    const f = await open();
    try {
      await f.commit(envelope());
      await f.commit(envelope());
      expect(await f.counts()).toEqual([2, 1, 1]);
      await expect(f.commit(envelope("one", "different"))).rejects.toThrow();
      expect(await f.counts()).toEqual([2, 1, 1]);
      await f.watchdog.tick();
      await f.commit(envelope());
      await f.watchdog.tick();
      expect(f.deliveries).toEqual(["one"]);
    } finally {
      await f.close();
    }
  });
  it("recovers a lost receiver response after reopening, with the same envelope identity", async () => {
    const f = await open();
    try {
      let first = true;
      f.receiver.run = () => {
        if (first) {
          first = false;
          return Promise.reject(new Error("answer lost"));
        }
        return Promise.resolve("accepted");
      };
      await f.commit(envelope());
      await f.watchdog.tick();
      expect(await f.post.read("one")).toMatchObject({
        state: "pending",
        attempts: 1,
      });
      f.clock.now += 2_000;
      const fresh = f.reopen();
      await fresh.post.enqueueDue();
      await fresh.watchdog.tick();
      expect(await fresh.post.read("one")).toEqual({ state: "delivered" });
      expect(f.deliveries).toEqual(["one", "one"]);
    } finally {
      await f.close();
    }
  });
  it("does not steal a live claim, and settles an accepted cancellation", async () => {
    const f = await open();
    try {
      let notify: () => void = () => undefined;
      let release: () => void = () => undefined;
      const started = new Promise<void>((resolve) => {
        notify = resolve;
      });
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      f.receiver.run = async () => {
        notify();
        await held;
        return "accepted";
      };
      await f.commit(envelope());
      const first = f.watchdog.tick();
      await Promise.race([started, first]);
      expect(await f.reopen().watchdog.tick()).toEqual({ status: "no_ready" });
      release();
      await first;
      expect(f.deliveries).toEqual(["one"]);
      await f.watchdog.ingest({
        jobId: "cancel-me",
        queue: "q",
        lane: "test",
        priority: 0,
        payload: null,
        state: "queued",
      });
      await f.watchdog.cancel({
        jobId: "cancel-me",
        cancellationId: "cancel-once",
      });
      expect(await f.watchdog.tick()).toMatchObject({
        status: "settled",
        job: { outcome: "cancelled" },
      });
    } finally {
      await f.close();
    }
  });
  it("expires a disappeared claim and rejects the old executor's late settlement", async () => {
    const f = await open();
    let release: () => void = () => undefined;
    try {
      let notify: () => void = () => undefined;
      const started = new Promise<void>((resolve) => {
        notify = resolve;
      });
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      f.receiver.run = async () => {
        notify();
        await held;
        return "accepted";
      };
      await f.commit(envelope());
      const old = f.watchdog.tick();
      await Promise.race([started, old]);
      const rejected = expect(old).rejects.toThrow("settlement rejected");
      f.clock.now += 120_001;
      const fresh = f.reopen();
      expect(await fresh.watchdog.tick()).toMatchObject({
        status: "settled",
        job: { outcome: "interrupted" },
      });
      release();
      await rejected;
      expect(await f.post.read("one")).toMatchObject({ state: "pending" });
      f.receiver.run = () => Promise.resolve("accepted");
      await fresh.post.enqueueDue();
      await fresh.watchdog.tick();
      expect(await fresh.post.read("one")).toEqual({ state: "delivered" });
    } finally {
      release();
      await f.close();
    }
  });
  it("fails the producer transaction on a conflicting Watchdog job identity", async () => {
    const f = await open();
    try {
      const job: WatchdogQueuedJob = {
        jobId: "stable",
        queue: "q",
        lane: "test",
        priority: 0,
        payload: { value: 1 },
        state: "queued",
      };
      await f.commitJob(job);
      await expect(
        f.commitJob({ ...job, payload: { value: 2 } }),
      ).rejects.toThrow();
      expect(await f.counts()).toEqual([1, 0, 1]);
      expect(await f.watchdog.read("stable")).toMatchObject({
        status: "found",
        job: { payload: { value: 1 } },
      });
    } finally {
      await f.close();
    }
  });
  it("renews a live private lease, then aborts rather than resurrecting an expired one", async () => {
    const f = await open();
    let signal: AbortSignal | undefined;
    let startedResolve: () => void = () => undefined;
    const started = new Promise<void>((resolve) => {
      startedResolve = resolve;
    });
    try {
      const worker = f.worker((_job, executionSignal) => {
        signal = executionSignal;
        startedResolve();
        return new Promise((_resolve, reject) => {
          executionSignal.addEventListener(
            "abort",
            () => {
              reject(new Error("aborted"));
            },
            { once: true },
          );
        });
      });
      await worker.ingest({
        jobId: "lease",
        queue: "q",
        lane: "test",
        priority: 0,
        payload: null,
        state: "queued",
      });
      const tick = worker.tick();
      await Promise.race([started, tick]);
      const rejected = expect(tick).rejects.toThrow("settlement rejected");
      const original = await f.lease("lease");
      f.clock.now += 1_000;
      await vi.waitFor(
        async () => {
          expect(await f.lease("lease")).toBeGreaterThan(original);
        },
        { timeout: 12_000, interval: 50 },
      );
      f.clock.now += 120_001;
      await vi.waitFor(
        () => {
          expect(signal?.aborted).toBe(true);
        },
        {
          timeout: 12_000,
          interval: 50,
        },
      );
      await rejected;
      expect(await worker.read("lease")).toMatchObject({
        status: "found",
        job: { state: "running" },
      });
      expect(await f.reopen().watchdog.tick()).toMatchObject({
        status: "settled",
        job: { outcome: "interrupted" },
      });
    } finally {
      await f.close();
    }
  }, 25_000);
  it("claims and settles using the database clock without a supplied proof clock", async () => {
    const f = await open();
    try {
      const worker = f.worker(
        () => Promise.resolve({ kind: "completed" }),
        true,
      );
      await worker.ingest({
        jobId: "native-clock",
        queue: "q",
        lane: "test",
        priority: 0,
        payload: null,
        state: "queued",
      });
      expect(await worker.tick()).toMatchObject({
        status: "settled",
        job: { outcome: "completed" },
      });
    } finally {
      await f.close();
    }
  });

  it("aborts an active executor and persists cancellation through the same engine", async () => {
    const f = await open();
    let notify: () => void = () => undefined;
    const started = new Promise<void>((resolve) => {
      notify = resolve;
    });
    let aborted = false;
    try {
      const worker = f.worker((_job, signal) => {
        notify();
        return new Promise((_resolve, reject) => {
          signal.addEventListener(
            "abort",
            () => {
              aborted = true;
              reject(new Error("cancelled"));
            },
            { once: true },
          );
        });
      });
      await worker.ingest({
        jobId: "active-cancel",
        queue: "q",
        lane: "test",
        priority: 0,
        payload: null,
        state: "queued",
      });
      const tick = worker.tick();
      await Promise.race([started, tick]);
      expect(
        await worker.cancel({ jobId: "active-cancel", cancellationId: "stop" }),
      ).toEqual({ status: "recorded" });
      expect(await tick).toMatchObject({
        status: "settled",
        job: { outcome: "cancelled" },
      });
      expect(aborted).toBe(true);
    } finally {
      await f.close();
    }
  });
  it("keeps permanent refusal as a dead letter rather than sending it again", async () => {
    const f = await open();
    try {
      f.receiver.run = () => Promise.resolve("refused");
      await f.commit(envelope());
      await f.watchdog.tick();
      expect(await f.post.read("one")).toMatchObject({
        state: "dead",
        reason: "refused",
        attempts: 1,
      });
      f.clock.now += 1_000_000;
      await f.post.enqueueDue();
      expect(await f.watchdog.tick()).toEqual({ status: "no_ready" });
      expect(f.deliveries).toEqual(["one"]);
    } finally {
      await f.close();
    }
  });
  it("holds a later envelope behind an earlier pending envelope on the same address", async () => {
    const f = await open();
    let release: () => void = () => undefined;
    try {
      let notify: () => void = () => undefined;
      const started = new Promise<void>((resolve) => {
        notify = resolve;
      });
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      f.receiver.run = async () => {
        notify();
        await held;
        return "accepted";
      };
      await f.commit(envelope());
      await f.commit(envelope("two"));
      const running = f.watchdog.tick();
      await Promise.race([started, running]);
      const other = f.reopen();
      await other.watchdog.tick();
      expect(f.deliveries).toEqual(["one"]);
      release();
      await running;
      f.receiver.run = () => Promise.resolve("accepted");
      await other.post.enqueueDue();
      await other.watchdog.tick();
      expect(f.deliveries).toEqual(["one", "two"]);
    } finally {
      release();
      await f.close();
    }
  });
  it("recovers within the declared budget and fences the replaced claim", async () => {
    const f = await open();
    let release: () => void = () => undefined;
    try {
      let notify: () => void = () => undefined;
      const started = new Promise<void>((resolve) => {
        notify = resolve;
      });
      const held = new Promise<void>((resolve) => {
        release = resolve;
      });
      const first = f.worker(async () => {
        notify();
        await held;
        return { kind: "failed" };
      });
      await first.ingest({
        jobId: "recoverable",
        queue: "q",
        lane: "test",
        priority: 0,
        payload: null,
        state: "queued",
        recovery: { maxRecoveries: Number.MAX_SAFE_INTEGER },
      });
      const old = first.tick();
      await Promise.race([started, old]);
      const rejected = expect(old).rejects.toThrow("settlement rejected");
      f.clock.now += 120_001;
      const recovered = f.worker(() => Promise.resolve({ kind: "completed" }));
      expect(await recovered.tick()).toMatchObject({
        status: "settled",
        job: { outcome: "completed" },
      });
      release();
      await rejected;
      expect(await recovered.read("recoverable")).toMatchObject({
        status: "found",
        job: { outcome: "completed" },
      });
    } finally {
      release();
      await f.close();
    }
  });
  it("reports a missing durable job rather than silently stranding its envelope", async () => {
    const f = await open();
    try {
      await f.commit(envelope());
      await f.watchdog.purge('["account","a"]');
      await expect(f.post.enqueueDue()).rejects.toThrow(
        "delivery job is missing",
      );
    } finally {
      await f.close();
    }
  });
}
