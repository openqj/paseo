export interface AgentTimelineReceiptIdentity {
  agentId: string;
  epoch: string;
  seq: number | null;
}

export interface AgentTimelineReceiptAck extends AgentTimelineReceiptIdentity {
  deliveryId: string;
  egressAtServerMs: number;
  acknowledgedAtServerMs: number;
  durationMs: number;
}

interface PendingReceipt {
  source: object;
  identity: AgentTimelineReceiptIdentity;
  createdAtMonotonicMs: number;
  egressAtMonotonicMs?: number;
  egressAtServerMs?: number;
}

/** Tracks bounded, one-shot client receipts using only the daemon's clocks. */
export class AgentTimelineReceiptTracker {
  private readonly pending = new Map<string, PendingReceipt>();

  constructor(
    private readonly monotonicNow: () => number = () => performance.now(),
    private readonly serverNow: () => number = () => Date.now(),
    private readonly maxPending = 4096,
    private readonly maxAgeMs = 120_000,
  ) {}

  register(deliveryId: string, source: object, identity: AgentTimelineReceiptIdentity): void {
    const now = this.monotonicNow();
    this.prune(now);
    this.pending.set(deliveryId, { source, identity, createdAtMonotonicMs: now });
    while (this.pending.size > this.maxPending) {
      const oldest = this.pending.keys().next().value;
      if (oldest === undefined) break;
      this.pending.delete(oldest);
    }
  }

  markSent(deliveryId: string, source: object): boolean {
    const receipt = this.pending.get(deliveryId);
    if (!receipt || receipt.source !== source) return false;
    receipt.egressAtMonotonicMs = this.monotonicNow();
    receipt.egressAtServerMs = this.serverNow();
    return true;
  }

  acknowledge(
    deliveryId: string,
    source: object,
    identity: AgentTimelineReceiptIdentity,
  ): AgentTimelineReceiptAck | null {
    const receipt = this.pending.get(deliveryId);
    if (!receipt || receipt.source !== source || receipt.egressAtMonotonicMs === undefined) {
      return null;
    }
    if (
      receipt.identity.agentId !== identity.agentId ||
      receipt.identity.epoch !== identity.epoch ||
      receipt.identity.seq !== identity.seq
    ) {
      return null;
    }

    this.pending.delete(deliveryId);
    const acknowledgedAtMonotonicMs = this.monotonicNow();
    const acknowledgedAtServerMs = this.serverNow();
    if (acknowledgedAtMonotonicMs - receipt.egressAtMonotonicMs > this.maxAgeMs) return null;
    return {
      ...receipt.identity,
      deliveryId,
      egressAtServerMs: receipt.egressAtServerMs!,
      acknowledgedAtServerMs,
      durationMs: Math.max(0, acknowledgedAtMonotonicMs - receipt.egressAtMonotonicMs),
    };
  }

  forgetSource(source: object): void {
    for (const [deliveryId, receipt] of this.pending) {
      if (receipt.source === source) this.pending.delete(deliveryId);
    }
  }

  get size(): number {
    return this.pending.size;
  }

  private prune(now: number): void {
    for (const [deliveryId, receipt] of this.pending) {
      const baseline = receipt.egressAtMonotonicMs ?? receipt.createdAtMonotonicMs;
      if (now - baseline > this.maxAgeMs) this.pending.delete(deliveryId);
    }
  }
}
