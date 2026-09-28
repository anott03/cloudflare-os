import { demuxGitFetchResponse, GitFetchSizeError } from "@gadgets/gatekeeper-kit/git-transport";

export const MAX_CHECKOUT_BYTES = 64 << 20;
const CHUNK_BYTES = 256 << 10;
const LIFETIME_MS = 10 * 60 * 1000;

type ExportState = {
  expiresAt: number;
  reader?: ReadableStreamDefaultReader<Uint8Array>;
};

export class CheckoutSpool {
  #storage: DurableObjectStorage;
  #active?: ExportState;

  constructor(storage: DurableObjectStorage) {
    this.#storage = storage;
    storage.sql.exec(`CREATE TABLE IF NOT EXISTS checkout_chunks (
      sequence INTEGER PRIMARY KEY,
      data BLOB NOT NULL
    )`);
    storage.sql.exec("DELETE FROM checkout_chunks");
  }

  async capture(load: () => Promise<ReadableStream<Uint8Array>>): Promise<ReadableStream<Uint8Array>> {
    if (this.#active) {
      throw new Error("A GitHub checkout export is already in progress. Consume or cancel its pack before retrying.");
    }
    const state: ExportState = { expiresAt: Date.now() + LIFETIME_MS };
    this.#active = state;
    try {
      await this.scheduleAlarm();
      const body = await load();
      if (this.#active !== state) {
        await body.cancel();
        throw new Error("GitHub checkout export expired. Please retry.");
      }
      const reader = demuxGitFetchResponse(body, MAX_CHECKOUT_BYTES).getReader();
      state.reader = reader;
      let chunk = new Uint8Array(CHUNK_BYTES);
      let used = 0;
      let count = 0;
      const store = async () => {
        this.#check(state);
        this.#storage.sql.exec("INSERT INTO checkout_chunks VALUES (?, ?)", count++, chunk.subarray(0, used));
        await this.#storage.sync();
        chunk = new Uint8Array(CHUNK_BYTES);
        used = 0;
      };
      while (true) {
        const next = await reader.read();
        this.#check(state);
        if (next.done) break;
        for (let offset = 0; offset < next.value.byteLength;) {
          const size = Math.min(CHUNK_BYTES - used, next.value.byteLength - offset);
          chunk.set(next.value.subarray(offset, offset + size), used);
          used += size;
          offset += size;
          if (used === CHUNK_BYTES) await store();
        }
      }
      if (used > 0) await store();
      this.#check(state);
      reader.releaseLock();
      state.reader = undefined;
      return this.#stream(state, count);
    } catch (error) {
      this.#clear(state);
      await state.reader?.cancel().catch(() => {});
      if (error instanceof GitFetchSizeError) {
        throw new Error("GitHub checkout export exceeds the 64 MiB transfer limit, including Git protocol framing.", {
          cause: error,
        });
      }
      throw error;
    }
  }

  async scheduleAlarm(): Promise<void> {
    const scheduled = await this.#storage.getAlarm();
    const state = this.#active;
    if (state && (scheduled === null || scheduled > state.expiresAt)) {
      await this.#storage.setAlarm(state.expiresAt);
    }
  }

  async alarm(): Promise<void> {
    const state = this.#active;
    if (!state) return;
    if (Date.now() < state.expiresAt) {
      await this.scheduleAlarm();
      return;
    }
    this.#clear(state);
    await state.reader?.cancel().catch(() => {});
  }

  #check(state: ExportState): void {
    if (this.#active !== state || Date.now() >= state.expiresAt) {
      throw new Error("GitHub checkout export expired. Please retry.");
    }
  }

  #clear(state: ExportState): void {
    if (this.#active !== state) return;
    this.#storage.sql.exec("DELETE FROM checkout_chunks");
    this.#active = undefined;
  }

  #stream(state: ExportState, count: number): ReadableStream<Uint8Array> {
    let sequence = 0;
    return new ReadableStream<Uint8Array>({
      pull: controller => {
        try {
          this.#check(state);
          if (sequence < count) {
            const row = this.#storage.sql.exec<{ data: ArrayBuffer }>(
              "SELECT data FROM checkout_chunks WHERE sequence = ?", sequence++).one();
            controller.enqueue(new Uint8Array(row.data));
          }
          if (sequence === count) {
            this.#clear(state);
            controller.close();
          }
        } catch (error) {
          this.#clear(state);
          controller.error(error);
        }
      },
      cancel: () => { this.#clear(state); },
    }, { highWaterMark: 0 });
  }
}
