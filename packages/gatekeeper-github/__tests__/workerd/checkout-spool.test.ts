import { RpcStub, RpcTarget } from "cloudflare:workers";
import { env, evictDurableObject, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { afterEach, describe, expect, it, vi } from "vitest";
import { CheckoutSpool, MAX_CHECKOUT_BYTES } from "../../src/checkout-spool";
import { encodePktLine, FLUSH_PKT } from "@gadgets/gatekeeper-kit/git-transport";

function frame(size: number, band = 1): Uint8Array {
  const bytes = new Uint8Array(size);
  bytes.set(new TextEncoder().encode(size.toString(16).padStart(4, "0")));
  bytes[4] = band;
  bytes.fill(42, 5);
  return bytes;
}

function response(size: number, band = 1, failure = false) {
  const header = encodePktLine("packfile");
  let remaining = size - header.byteLength - FLUSH_PKT.byteLength;
  let started = false;
  const cancel = vi.fn();
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      if (!started) {
        started = true;
        controller.enqueue(header);
      } else if (remaining > 0) {
        const length = Math.min(65535, remaining);
        controller.enqueue(frame(length, band));
        remaining -= length;
      } else if (failure) {
        controller.error(new Error("upstream failed"));
      } else {
        controller.enqueue(FLUSH_PKT);
        controller.close();
      }
    },
    cancel,
  }, { highWaterMark: 0 });
  return { body, cancel };
}

function stored(storage: DurableObjectStorage) {
  return storage.sql.exec<{ chunks: number; bytes: number; largest: number }>(
    `SELECT count(*) AS chunks, coalesce(sum(length(data)), 0) AS bytes,
      coalesce(max(length(data)), 0) AS largest FROM checkout_chunks`).one();
}

async function inStorage(test: (storage: DurableObjectStorage) => Promise<void>) {
  const stub = env.TEST_HOOKS.get(env.TEST_HOOKS.newUniqueId());
  await runInDurableObject(stub, async (_instance, state) => {
    await test(state.storage);
  });
}

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

const COMMIT = "a".repeat(40);

class CheckoutCache extends RpcTarget {
  async consumePack(pack: ReadableStream<Uint8Array>): Promise<string[]> {
    await new Response(pack).arrayBuffer();
    return [COMMIT];
  }
}

class CheckoutQueue extends RpcTarget {
  async getGitCache() { return new CheckoutCache(); }
  async authorizeObservation(): Promise<void> {}
}

async function exportCheckout(size: number) {
  const accountId = env.USER_ACCOUNT.newUniqueId();
  await runInDurableObject(env.USER_ACCOUNT.get(accountId), async (_instance, state) => {
    state.storage.kv.put("accessToken", "checkout-test-token");
  });
  let fetches = 0;
  vi.stubGlobal("fetch", async (input: RequestInfo | URL, init?: RequestInit) => {
    expect(new Headers(init?.headers).get("Authorization")).toBeTruthy();
    const url = input instanceof Request ? input.url : input.toString();
    if (url.includes("/commits/")) return new Response(COMMIT);
    expect(url).toBe("https://github.com/acme/widgets.git/git-upload-pack");
    return new Response(response(++fetches === 1 ? 100 : size).body);
  });
  const hooks = env.TEST_HOOKS.get(env.TEST_HOOKS.newUniqueId());
  using queueStub = new RpcStub(new CheckoutQueue());
  // SAFETY: exportCheckout only calls the implemented queue/cache methods; the hook's
  // Workers RPC parameter type cannot represent the generic ApprovalQueue.bindHook method.
  const queue = queueStub as never;
  return await hooks.exportCheckout("checkout", {
    userObjectId: accountId.toString(), resourceKind: "repo", owner: "acme", repo: "widgets",
  }, queue, COMMIT);
}

describe("exportCheckout RPC", () => {
  it("returns the size failure on the method call, not a broken pack stream", async () => {
    const result = await exportCheckout(MAX_CHECKOUT_BYTES + 1);
    expect(result).toEqual({
      error: "GitHub checkout export exceeds the 64 MiB transfer limit, including Git protocol framing.",
    });
  }, 30_000);

  it("returns the unchanged checkout API with a readable pack", async () => {
    const result = await exportCheckout(1 << 20);
    if ("error" in result) throw new Error(result.error);
    expect(result.ok.commitId).toBe(COMMIT);
    const bytes = new Uint8Array(await new Response(result.ok.pack).arrayBuffer());
    expect(bytes.byteLength).toBeGreaterThan(1_000_000);
    expect(bytes.every(byte => byte === 42)).toBe(true);
  });
});

describe("checkout spool", () => {
  it("rejects an oversized framed response before returning an output stream", async () => {
    await inStorage(async storage => {
      const spool = new CheckoutSpool(storage);
      const input = response(MAX_CHECKOUT_BYTES + 1);
      await expect(spool.capture(async () => input.body)).rejects.toThrow(
        "GitHub checkout export exceeds the 64 MiB transfer limit");
      expect(stored(storage).bytes).toBe(0);
      const retry = await spool.capture(async () => response(100).body);
      await retry.cancel();
    });
  }, 30_000);

  it("accepts the exact transport limit and bounds storage chunks", async () => {
    await inStorage(async storage => {
      const spool = new CheckoutSpool(storage);
      const output = await spool.capture(async () => response(MAX_CHECKOUT_BYTES).body);
      const before = stored(storage);
      expect(before.bytes).toBeLessThan(MAX_CHECKOUT_BYTES);
      expect(before.chunks).toBeLessThanOrEqual(256);
      expect(before.largest).toBe(256 << 10);
      let received = 0;
      const reader = output.getReader();
      while (true) {
        const next = await reader.read();
        if (next.done) break;
        expect(next.value.every(byte => byte === 42)).toBe(true);
        received += next.value.byteLength;
      }
      expect(received).toBe(before.bytes);
      expect(stored(storage).bytes).toBe(0);
    });
  }, 30_000);

  it("counts progress framing toward the limit, not just pack bytes", async () => {
    await inStorage(async storage => {
      const spool = new CheckoutSpool(storage);
      await expect(spool.capture(async () => response(MAX_CHECKOUT_BYTES + 1, 2).body))
        .rejects.toThrow("64 MiB transfer limit");
      expect(stored(storage).bytes).toBe(0);
    });
  }, 30_000);

  it("removes partial data when a storage write fails", async () => {
    await inStorage(async storage => {
      const spool = new CheckoutSpool(storage);
      storage.sql.exec(`CREATE TRIGGER reject_checkout BEFORE INSERT ON checkout_chunks
        WHEN NEW.sequence = 1 BEGIN SELECT RAISE(ABORT, 'spool write failed'); END`);
      const input = response(1 << 20);
      await expect(spool.capture(async () => input.body)).rejects.toThrow("spool write failed");
      expect(stored(storage).bytes).toBe(0);
      expect(input.cancel).toHaveBeenCalledOnce();
      storage.sql.exec("DROP TRIGGER reject_checkout");
      await (await spool.capture(async () => response(100).body)).cancel();
    });
  });

  it("removes partial data on upstream failure and permits a retry", async () => {
    await inStorage(async storage => {
      const spool = new CheckoutSpool(storage);
      await expect(spool.capture(async () => response(1 << 20, 1, true).body))
        .rejects.toThrow("upstream failed");
      expect(stored(storage).bytes).toBe(0);
      const output = await spool.capture(async () => response(100).body);
      await output.cancel();
      expect(stored(storage).bytes).toBe(0);
    });
  });

  it("rejects concurrent exports without loading them and cleans up cancelled output", async () => {
    await inStorage(async storage => {
      const spool = new CheckoutSpool(storage);
      const output = await spool.capture(async () => response(1 << 20).body);
      const load = vi.fn(async () => response(100).body);
      await expect(spool.capture(load)).rejects.toThrow("already in progress");
      expect(load).not.toHaveBeenCalled();
      const reader = output.getReader();
      expect((await reader.read()).done).toBe(false);
      await reader.cancel();
      expect(stored(storage).bytes).toBe(0);
      await (await spool.capture(load)).cancel();
    });
  });

  it("expires abandoned output and does not let its cancellation delete a newer export", async () => {
    await inStorage(async storage => {
      const spool = new CheckoutSpool(storage);
      const abandoned = await spool.capture(async () => response(1 << 20).body);
      await spool.alarm();
      expect(stored(storage).bytes).toBeGreaterThan(0);
      const deadline = await storage.getAlarm();
      expect(deadline).not.toBeNull();
      vi.spyOn(Date, "now").mockReturnValue(deadline ?? 0);
      await spool.alarm();
      expect(stored(storage).bytes).toBe(0);
      const next = await spool.capture(async () => response(1 << 20).body);
      await abandoned.cancel();
      expect(stored(storage).bytes).toBeGreaterThan(0);
      await next.cancel();
    });
  });

  it("cancels a stalled download on expiry without resurrecting its rows", async () => {
    await inStorage(async storage => {
      const spool = new CheckoutSpool(storage);
      let ready!: () => void;
      const reading = new Promise<void>(resolve => { ready = resolve; });
      const cancel = vi.fn();
      const body = new ReadableStream<Uint8Array>({
        pull() { ready(); },
        cancel,
      }, { highWaterMark: 0 });
      const capture = spool.capture(async () => body);
      const rejected = expect(capture).rejects.toThrow("expired");
      await reading;
      const deadline = await storage.getAlarm();
      vi.spyOn(Date, "now").mockReturnValue(deadline ?? 0);
      await spool.alarm();
      await rejected;
      expect(cancel).toHaveBeenCalledOnce();
      expect(stored(storage).bytes).toBe(0);
    });
  });

  it("preserves an earlier account alarm and can restore its own deadline", async () => {
    await inStorage(async storage => {
      const earlier = Date.now() + 60_000;
      await storage.setAlarm(earlier);
      const spool = new CheckoutSpool(storage);
      const output = await spool.capture(async () => response(100).body);
      expect(await storage.getAlarm()).toBe(earlier);
      await storage.deleteAlarm();
      await spool.scheduleAlarm();
      expect(await storage.getAlarm()).toBeGreaterThan(earlier);
      await output.cancel();
    });
  });

  it("cleans abandoned account storage through its alarm without deleting credentials", async () => {
    const account = env.USER_ACCOUNT.get(env.USER_ACCOUNT.newUniqueId());
    const deadline = await runInDurableObject(account, async (instance, state) => {
      state.storage.kv.put("accessToken", "unrelated");
      await instance.spoolCheckoutPack(response(1 << 20).body);
      expect(stored(state.storage).bytes).toBeGreaterThan(0);
      return await state.storage.getAlarm();
    });
    expect(await runDurableObjectAlarm(account)).toBe(true);
    await runInDurableObject(account, async (_instance, state) => {
      expect(stored(state.storage).bytes).toBeGreaterThan(0);
      expect(await state.storage.getAlarm()).toBe(deadline);
    });
    vi.spyOn(Date, "now").mockReturnValue(deadline ?? 0);
    expect(await runDurableObjectAlarm(account)).toBe(true);
    await runInDurableObject(account, async (_instance, state) => {
      expect(stored(state.storage).bytes).toBe(0);
    });
    expect(await account.getAccessToken()).toBe("unrelated");
  });

  it("cleans orphaned chunks when the account restarts", async () => {
    const account = env.USER_ACCOUNT.get(env.USER_ACCOUNT.newUniqueId());
    await runInDurableObject(account, async (_instance, state) => {
      state.storage.kv.put("accessToken", "unrelated");
      state.storage.sql.exec("INSERT INTO checkout_chunks VALUES (?, ?)", 0, new Uint8Array(256 << 10));
      await state.storage.setAlarm(Date.now() + 600_000);
    });
    await evictDurableObject(account);
    expect(await account.getAccessToken()).toBe("unrelated");
    await runInDurableObject(account, async (_instance, state) => {
      expect(stored(state.storage).bytes).toBe(0);
    });
  });
});
