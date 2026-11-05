import {
  buildRelayWebSocketUrl,
  shouldUseTlsForDefaultHostedRelay,
} from "@getpaseo/protocol/daemon-endpoints";
import {
  ConnectionOfferSchema,
  parseConnectionOfferFromUrl,
  type ConnectionOffer,
} from "@getpaseo/protocol/connection-offer";
import type { AgentProvider } from "@getpaseo/protocol/agent-types";
import { CLIENT_CAPS } from "@getpaseo/protocol/client-capabilities";
import {
  DaemonClient,
  type CreateAgentRequestOptions,
  type FetchRecentProviderSessionEntry,
} from "./daemon-client.js";
import type { TimelineSubscription } from "./connection/index.js";

type AndroidCommand = {
  requestId?: string;
  operation: string;
  hostId?: string;
  offerUrl?: string;
  connectionOffer?: unknown;
  identityRequired?: boolean;
  clientId?: string;
  agentId?: string;
  text?: string;
  messageId?: string;
  provider?: string;
  providerId?: string;
  providerHandleId?: string;
  cwd?: string;
  workspaceId?: string;
  title?: string;
  model?: string;
  initialPrompt?: string;
  path?: string;
  limit?: number;
  query?: string;
  direction?: "tail" | "before" | "after";
  cursor?: { epoch: string; seq: number };
  projection?: "projected" | "canonical";
  trackDeliveryReceipt?: boolean;
  epoch?: string;
  seq?: number;
  deliveryId?: string;
};

type AndroidWindow = Window & {
  AndroidPaseoBridge?: {
    postMessage(message: string): void;
    getIdentityToken(requestId: string): void;
  };
  PaseoAndroidBridge?: {
    handle(message: string): void;
    resolveIdentityToken(requestId: string, token: string | null, error: string | null): void;
  };
};

type ConnectedHost = {
  client: DaemonClient;
  offer: ConnectionOffer;
  removeStatusListener: () => void;
  removeAgentUpdateListener: () => void;
  agentUpdatesSubscription?: ReturnType<DaemonClient["observeAgents"]>;
};

const clients = new Map<string, ConnectedHost>();
const timelineSubscriptions = new Map<string, TimelineSubscription>();
const pendingIdentityTokens = new Map<
  string,
  { resolve: (token: string) => void; reject: (error: Error) => void }
>();
let nextIdentityTokenRequestId = 1;
const INITIAL_CONNECTION_TIMEOUT_MS = 30_000;

function withTimeout<T>(operation: Promise<T>, timeoutMs: number, message: string): Promise<T> {
  let timeout: number | undefined;
  return Promise.race([
    operation,
    new Promise<T>((_, reject) => {
      timeout = window.setTimeout(() => reject(new Error(message)), timeoutMs);
    }),
  ]).finally(() => {
    if (timeout !== undefined) window.clearTimeout(timeout);
  });
}

function emit(message: Record<string, unknown>): void {
  (window as AndroidWindow).AndroidPaseoBridge?.postMessage(JSON.stringify(message));
}

function reply(requestId: string | undefined, data?: unknown, error?: unknown): void {
  emit({
    type: "response",
    requestId,
    ok: error === undefined,
    ...(data === undefined ? {} : { data }),
    ...(error === undefined
      ? {}
      : { error: error instanceof Error ? error.message : String(error) }),
  });
}

function requireText(value: string | undefined, field: string): string {
  const normalized = value?.trim();
  if (!normalized) throw new Error(`${field} is required`);
  return normalized;
}

function requireClient(hostId: string | undefined): ConnectedHost {
  const id = requireText(hostId, "hostId");
  const host = clients.get(id);
  if (!host) throw new Error("Paseo host is not connected");
  return host;
}

function publishTimeline(hostId: string, message: unknown): void {
  emit({ type: "timeline", hostId, message });
}

async function closeHost(hostId: string): Promise<void> {
  for (const [key, subscription] of timelineSubscriptions) {
    if (key.startsWith(`${hostId}:`)) {
      timelineSubscriptions.delete(key);
      await subscription.release().catch(() => undefined);
    }
  }
  const host = clients.get(hostId);
  if (!host) return;
  clients.delete(hostId);
  host.removeStatusListener();
  host.removeAgentUpdateListener();
  await host.client.close();
  await host.agentUpdatesSubscription?.release().catch(() => undefined);
}

function requestIdentityToken(): Promise<string> {
  const bridge = (window as AndroidWindow).AndroidPaseoBridge;
  if (!bridge?.getIdentityToken)
    return Promise.reject(new Error("Android Identity bridge is unavailable"));
  const requestId = `identity-${nextIdentityTokenRequestId++}`;
  return new Promise<string>((resolve, reject) => {
    const timeout = window.setTimeout(() => {
      pendingIdentityTokens.delete(requestId);
      reject(new Error("Identity token request timed out"));
    }, 15_000);
    pendingIdentityTokens.set(requestId, {
      resolve: (token) => {
        window.clearTimeout(timeout);
        resolve(token);
      },
      reject: (error) => {
        window.clearTimeout(timeout);
        reject(error);
      },
    });
    bridge.getIdentityToken(requestId);
  });
}

async function connect(command: AndroidCommand): Promise<unknown> {
  const hostId = requireText(command.hostId, "hostId");
  const offer = command.connectionOffer
    ? ConnectionOfferSchema.parse(command.connectionOffer)
    : parseConnectionOfferFromUrl(requireText(command.offerUrl, "offerUrl"));
  if (!offer) throw new Error("The URL does not contain a Paseo pairing offer");
  if (offer.serverId !== hostId) throw new Error("hostId does not match the Paseo pairing offer");

  await closeHost(hostId);
  const url = buildRelayWebSocketUrl({
    endpoint: offer.relay.endpoint,
    useTls: offer.relay.useTls ?? shouldUseTlsForDefaultHostedRelay(offer.relay.endpoint),
    serverId: offer.serverId,
    role: "client",
  });
  const client = new DaemonClient({
    url,
    clientId: command.clientId?.trim() || `android-${crypto.randomUUID()}`,
    clientType: "mobile",
    capabilities: { [CLIENT_CAPS.agentTimelineDeliveryAcks]: true },
    e2ee: { enabled: true, daemonPublicKeyB64: offer.daemonPublicKeyB64 },
    ...(command.identityRequired ? { identityTokenProvider: requestIdentityToken } : {}),
    reconnect: { enabled: true },
  });
  const removeStatusListener = client.subscribeConnectionStatus((status) => {
    emit({ type: "connection", hostId, status });
  });
  client.on("agent_stream", (message) => publishTimeline(hostId, message));
  const removeAgentUpdateListener = client.on("agent_update", (message) => {
    emit({ type: "agentUpdate", hostId, payload: message.payload });
  });
  const connected: ConnectedHost = {
    client,
    offer,
    removeStatusListener,
    removeAgentUpdateListener,
  };
  clients.set(hostId, connected);
  try {
    await withTimeout(
      client.connect(),
      INITIAL_CONNECTION_TIMEOUT_MS,
      "Paseo host did not become ready within 30 seconds",
    );
    connected.agentUpdatesSubscription = client.observeAgents();
    void connected.agentUpdatesSubscription.ready.catch(() => undefined);
  } catch (error) {
    await closeHost(hostId);
    throw error;
  }
  return { hostId, serverId: offer.serverId, status: client.getConnectionState() };
}

async function handle(command: AndroidCommand): Promise<unknown> {
  switch (command.operation) {
    case "connect":
      return connect(command);
    case "disconnect":
      await closeHost(requireText(command.hostId, "hostId"));
      return { disconnected: true };
    case "fetchAgents": {
      const { client } = requireClient(command.hostId);
      const entries = [];
      let cursor: string | undefined;
      do {
        const response = await client.fetchAgents({
          page: { limit: 200, ...(cursor ? { cursor } : {}) },
        });
        entries.push(...response.entries);
        cursor = response.pageInfo.hasMore
          ? (response.pageInfo.nextCursor ?? undefined)
          : undefined;
      } while (cursor);
      return { entries };
    }
    case "fetchWorkspaces": {
      const { client } = requireClient(command.hostId);
      const entries = [];
      let cursor: string | undefined;
      let emptyProjects: unknown;
      do {
        const response = await client.fetchWorkspaces({
          page: { limit: 200, ...(cursor ? { cursor } : {}) },
        });
        entries.push(...response.entries);
        emptyProjects = response.emptyProjects;
        cursor = response.pageInfo.hasMore
          ? (response.pageInfo.nextCursor ?? undefined)
          : undefined;
      } while (cursor);
      return { entries, emptyProjects };
    }
    case "createWorkspace": {
      const { client } = requireClient(command.hostId);
      const result = await client.createWorkspace({
        source: { kind: "directory", path: requireText(command.path, "path") },
        ...(command.title?.trim() ? { title: command.title.trim() } : {}),
      });
      if (result.error || !result.workspace) {
        throw new Error(result.error ?? "Paseo did not create the project");
      }
      return result;
    }
    case "fetchProviders": {
      const { client } = requireClient(command.hostId);
      const response = await client.getProvidersSnapshot();
      return { entries: response.entries };
    }
    case "fetchRecentProviderSessions": {
      const { client } = requireClient(command.hostId);
      const providerSnapshot = (await client.getProvidersSnapshot()).entries
        .filter((entry) => entry.enabled !== false)
        .map((entry) => ({
          id: entry.provider,
          label: entry.label?.trim() || entry.provider,
          provider: entry.provider,
        }));
      const providers = providerSnapshot.map((entry) => entry.provider);
      const limit = Math.min(Math.max(command.limit ?? 15, 1), 200);
      const query = command.query?.trim();
      const results = await Promise.all(
        providers.map(async (provider) => {
          try {
            return {
              status: "success" as const,
              provider,
              response: await client.fetchRecentProviderSessions({
                cwd: command.cwd,
                providers: [provider],
                limit,
                ...(query ? { query } : {}),
              }),
            };
          } catch (error) {
            return {
              status: "error" as const,
              provider,
              error: error instanceof Error ? error.message : String(error),
            };
          }
        }),
      );
      const entries: FetchRecentProviderSessionEntry[] = [];
      const providerErrors: Array<{ provider: string; message: string }> = [];
      let filteredAlreadyImportedCount = 0;
      let hasMore = false;
      for (const result of results) {
        if (result.status === "error") {
          providerErrors.push({ provider: result.provider, message: result.error });
          continue;
        }
        entries.push(...result.response.entries);
        hasMore ||= result.response.entries.length >= limit;
        filteredAlreadyImportedCount += result.response.filteredAlreadyImportedCount ?? 0;
        providerErrors.push(...(result.response.providerErrors ?? []));
      }
      entries.sort(
        (a, b) => new Date(b.lastActivityAt).getTime() - new Date(a.lastActivityAt).getTime(),
      );
      return {
        entries,
        providers: providerSnapshot.map(({ id, label }) => ({ id, label })),
        hasMore,
        ...(filteredAlreadyImportedCount > 0 ? { filteredAlreadyImportedCount } : {}),
        ...(providerErrors.length > 0 ? { providerErrors } : {}),
      };
    }
    case "fetchAgent": {
      const { client } = requireClient(command.hostId);
      const result = await client.fetchAgent(requireText(command.agentId, "agentId"));
      if (!result) throw new Error("Paseo Agent session was not found");
      return result;
    }
    case "fetchTimeline": {
      const { client } = requireClient(command.hostId);
      return client.fetchAgentTimeline(requireText(command.agentId, "agentId"), {
        direction: command.direction ?? "tail",
        ...(command.cursor ? { cursor: command.cursor } : {}),
        limit: Math.min(Math.max(command.limit ?? 100, 1), 200),
        ...(command.projection ? { projection: command.projection } : {}),
        ...(command.trackDeliveryReceipt === true ? { trackDeliveryReceipt: true } : {}),
      });
    }
    case "ackTimelineDelivery": {
      const { client } = requireClient(command.hostId);
      client.acknowledgeAgentTimelineDelivery({
        agentId: requireText(command.agentId, "agentId"),
        deliveryId: requireText(command.deliveryId, "deliveryId"),
        epoch: requireText(command.epoch, "epoch"),
        ...(typeof command.seq === "number" && command.seq >= 0 ? { seq: command.seq } : {}),
      });
      return { acknowledged: true };
    }
    case "subscribeTimeline": {
      const hostId = requireText(command.hostId, "hostId");
      const agentId = requireText(command.agentId, "agentId");
      const { client } = requireClient(hostId);
      const key = `${hostId}:${agentId}`;
      const previous = timelineSubscriptions.get(key);
      if (previous) await previous.release();
      const subscription = client.subscribeAgentTimeline(agentId, (message) =>
        publishTimeline(hostId, message),
      );
      timelineSubscriptions.set(key, subscription);
      await subscription.ready;
      return { subscribed: true, agentId };
    }
    case "unsubscribeTimeline": {
      const key = `${requireText(command.hostId, "hostId")}:${requireText(command.agentId, "agentId")}`;
      const subscription = timelineSubscriptions.get(key);
      if (subscription) {
        timelineSubscriptions.delete(key);
        await subscription.release();
      }
      return { subscribed: false };
    }
    case "createAgent": {
      const { client } = requireClient(command.hostId);
      const provider = requireText(command.provider, "provider") as AgentProvider;
      const cwd = requireText(command.cwd, "cwd");
      const options: CreateAgentRequestOptions = {
        provider,
        cwd,
        ...(command.workspaceId ? { workspaceId: command.workspaceId } : {}),
        ...(command.title ? { title: command.title } : {}),
        ...(command.model ? { model: command.model } : {}),
        ...(command.initialPrompt ? { initialPrompt: command.initialPrompt } : {}),
      };
      return client.createAgent(options);
    }
    case "importAgent": {
      const { client } = requireClient(command.hostId);
      return client.importAgent({
        providerId: requireText(command.providerId, "providerId"),
        providerHandleId: requireText(command.providerHandleId, "providerHandleId"),
        cwd: requireText(command.cwd, "cwd"),
        ...(command.workspaceId ? { workspaceId: command.workspaceId } : {}),
      });
    }
    case "sendAgentMessage": {
      const { client } = requireClient(command.hostId);
      const agentId = requireText(command.agentId, "agentId");
      const snapshot = await client.fetchAgent(agentId);
      if (!snapshot) throw new Error("Paseo Agent session was not found");
      if (snapshot.agent.status === "closed") {
        if (!snapshot.agent.persistence)
          throw new Error("This closed session cannot be resumed by the provider");
        await client.resumeAgent(snapshot.agent.persistence);
      }
      await client.sendAgentMessage(agentId, requireText(command.text, "text"), {
        ...(command.messageId ? { messageId: command.messageId } : {}),
      });
      return { accepted: true, agentId };
    }
    default:
      throw new Error("Unsupported Paseo bridge operation");
  }
}

(window as AndroidWindow).PaseoAndroidBridge = {
  handle(message: string): void {
    let command: AndroidCommand;
    try {
      command = JSON.parse(message) as AndroidCommand;
    } catch (error) {
      reply(undefined, undefined, error);
      return;
    }
    void handle(command).then(
      (data) => reply(command.requestId, data),
      (error: unknown) => reply(command.requestId, undefined, error),
    );
  },
  resolveIdentityToken(requestId: string, token: string | null, error: string | null): void {
    const pending = pendingIdentityTokens.get(requestId);
    if (!pending) return;
    pendingIdentityTokens.delete(requestId);
    if (error || !token) pending.reject(new Error(error || "Identity token is unavailable"));
    else pending.resolve(token);
  },
};

emit({ type: "ready" });
