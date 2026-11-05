import { describe, expect, it } from "vitest";
import { AgentTimelineReceiptTracker } from "./agent-timeline-receipts.js";

describe("AgentTimelineReceiptTracker", () => {
  it("measures send-to-ack with daemon clocks and validates source and cursor", () => {
    let monotonicMs = 100;
    let serverMs = 1_800_000_000_000;
    const tracker = new AgentTimelineReceiptTracker(
      () => monotonicMs,
      () => serverMs,
    );
    const source = {};
    const otherSource = {};
    const identity = { agentId: "agent-1", epoch: "epoch-1", seq: 4 };

    tracker.register("delivery-1", source, identity);
    expect(tracker.acknowledge("delivery-1", source, identity)).toBeNull();
    expect(tracker.markSent("delivery-1", otherSource)).toBe(false);
    expect(tracker.markSent("delivery-1", source)).toBe(true);

    monotonicMs += 25;
    serverMs += 25;
    expect(tracker.acknowledge("delivery-1", otherSource, identity)).toBeNull();
    expect(tracker.acknowledge("delivery-1", source, { ...identity, seq: 5 })).toBeNull();
    expect(tracker.acknowledge("delivery-1", source, identity)).toEqual({
      ...identity,
      deliveryId: "delivery-1",
      egressAtServerMs: 1_800_000_000_000,
      acknowledgedAtServerMs: 1_800_000_000_025,
      durationMs: 25,
    });
    expect(tracker.acknowledge("delivery-1", source, identity)).toBeNull();
  });

  it("keeps pending receipts bounded and removes receipts with their source", () => {
    let now = 0;
    const tracker = new AgentTimelineReceiptTracker(
      () => now,
      () => now,
      2,
      10,
    );
    const source = {};
    tracker.register("old", source, { agentId: "a", epoch: "e", seq: 1 });
    tracker.register("new", source, { agentId: "a", epoch: "e", seq: 2 });
    tracker.register("latest", source, { agentId: "a", epoch: "e", seq: 3 });
    expect(tracker.size).toBe(2);
    expect(tracker.markSent("old", source)).toBe(false);
    now = 11;
    tracker.register("fresh", source, { agentId: "a", epoch: "e", seq: 4 });
    expect(tracker.size).toBe(1);
    tracker.forgetSource(source);
    expect(tracker.size).toBe(0);
  });
});
