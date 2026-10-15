import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/*.test.ts", "test/*.test.mjs"],
    exclude: ["test/*.cloudflare.test.mjs"],
  },
});
