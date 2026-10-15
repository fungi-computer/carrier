import { DurableObject } from "cloudflare:workers";
import { createCloudflareCarrier } from "../src/cloudflare.js";
import { createDeliveryInspection } from "../src/inspection.js";

// Native SQLite/alarms with controlled provider I/O. No send-email binding exists.
export class CarrierFixture extends DurableObject {
  owner;
  mode;
  failNextTransaction = false;
  storage = {
    sql: {
      exec: (statement, ...bindings) => {
        const result = this.ctx.storage.sql.exec(statement, ...bindings);
        if (
          this.mode === "receipt_saved" &&
          statement.startsWith("UPDATE carrier_emails SET status=") &&
          bindings[0] === "provider_accepted"
        ) {
          this.mode = undefined;
          this.failNextTransaction = true;
        }
        return result;
      },
    },
    transactionSync: (operation) => {
      if (this.failNextTransaction) {
        this.failNextTransaction = false;
        throw new Error("Controlled storage interruption");
      }
      return this.ctx.storage.transactionSync(operation);
    },
    transaction: (operation) => this.ctx.storage.transaction(operation),
    getAlarm: () => this.ctx.storage.getAlarm(),
    setAlarm: (when) => {
      if (this.mode === "acceptance_wake_failed") {
        this.mode = undefined;
        throw new Error("Controlled wake write failure");
      }
      return this.ctx.storage.setAlarm(when);
    },
    deleteAlarm: () => this.ctx.storage.deleteAlarm(),
  };

  carrier() {
    this.owner ??= createCloudflareCarrier({
      storage: this.storage,
      canSend: async (emailId) => {
        const checks =
          (await this.ctx.storage.get("fixture:validity-checks")) ?? [];
        checks.push(emailId);
        await this.ctx.storage.put("fixture:validity-checks", checks);
        if (this.mode === "validity_unavailable")
          throw new Error("Source unavailable");
        if (this.mode === "superseded") return false;
        if (this.mode === "before_dispatch") {
          this.mode = undefined;
          this.failNextTransaction = true;
        }
        return true;
      },
      email: {
        send: async (message) => {
          const calls =
            (await this.ctx.storage.get("fixture:provider-calls")) ?? [];
          calls.push(message);
          await this.ctx.storage.put("fixture:provider-calls", calls);
          if (this.mode === "interrupted_send") {
            await this.ctx.storage.sync();
            this.ctx.abort("Controlled instance termination after dispatch");
          }
          if (this.mode === "response_lost")
            throw new Error("Controlled lost acknowledgment");
          return { messageId: "controlled-provider-receipt" };
        },
      },
    });
    return this.owner;
  }

  async send(input) {
    const receipt = await (await this.carrier()).send(input);
    if (this.mode === "acceptance_response_lost") {
      await this.ctx.storage.sync();
      this.ctx.abort("Controlled loss of the durable acceptance response");
    }
    return receipt;
  }

  async get(id) {
    return (await this.carrier()).get(id);
  }

  receipts(query) {
    return createDeliveryInspection(this.ctx.storage.sql).email(query);
  }

  configure(mode) {
    this.mode = mode;
  }

  async alarm() {
    await (await this.carrier()).alarm();
  }

  async drain() {
    await this.alarm();
  }

  async inspect() {
    return {
      calls: (await this.ctx.storage.get("fixture:provider-calls")) ?? [],
      validityChecks:
        (await this.ctx.storage.get("fixture:validity-checks")) ?? [],
      alarm: await this.ctx.storage.getAlarm(),
    };
  }
}

// The same native host accepts another transport without an email binding.
export class CarrierTransportFixture extends DurableObject {
  owner = createCloudflareCarrier({
    storage: this.ctx.storage,
    transport: async (email) => {
      const calls =
        (await this.ctx.storage.get("fixture:transport-calls")) ?? [];
      calls.push(email);
      await this.ctx.storage.put("fixture:transport-calls", calls);
      return {
        kind: "provider_accepted",
        providerMessageId: "alternate-provider-receipt",
      };
    },
  });

  async send(input) {
    return (await this.owner).send(input);
  }

  async get(id) {
    return (await this.owner).get(id);
  }

  async alarm() {
    await (await this.owner).alarm();
  }

  async inspect() {
    return {
      calls: (await this.ctx.storage.get("fixture:transport-calls")) ?? [],
      alarm: await this.ctx.storage.getAlarm(),
    };
  }
}

export default {
  fetch: () => new globalThis.Response("Carrier qualification fixture"),
};
