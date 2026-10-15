import { cloudflareTest } from "@cloudflare/vitest-plugin";
import { defineConfig } from "vitest/config";

export default defineConfig({
  root: import.meta.dirname,
  plugins: [
    cloudflareTest({
      main: "./test/carrier.worker.mjs",
      remoteBindings: false,
      miniflare: {
        compatibilityDate: "2026-08-30",
        d1Databases: ["POST_DB"],
        durableObjects: {
          CARRIER: { className: "CarrierFixture", useSQLite: true },
          CARRIER_TRANSPORT: {
            className: "CarrierTransportFixture",
            useSQLite: true,
          },
        },
      },
    }),
  ],
  test: { include: ["test/*.cloudflare.test.mjs"] },
});
