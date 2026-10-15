/** String-row outbox for deterministic tests and quick starts; no persistence. */
export type MemoryOutbox = Readonly<{
  /** Store or replace one encoded row. */
  put: (key: string, value: string) => Promise<void>;
  /** Read a retained row, if present. */
  read: (key: string) => Promise<string | undefined>;
  /** Read a key-ordered page strictly after the supplied cursor. */
  list: (
    limit: number,
    afterKey?: string,
  ) => Promise<readonly Readonly<{ key: string; value: string }>[]>;
  /** Remove one row. */
  delete: (key: string) => Promise<void>;
}>;

/** Create an independent in-process outbox for trusted tests and examples. */
export function createMemoryOutbox(): MemoryOutbox {
  const rows = new Map<string, string>();
  return {
    put: (key, value) => {
      rows.set(key, value);
      return Promise.resolve();
    },
    read: (key) => Promise.resolve(rows.get(key)),
    list: (limit, afterKey) =>
      Promise.resolve(
        [...rows]
          .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
          .filter(([key]) => afterKey === undefined || key > afterKey)
          .slice(0, limit)
          .map(([key, value]) => ({ key, value })),
      ),
    delete: (key) => {
      rows.delete(key);
      return Promise.resolve();
    },
  };
}
