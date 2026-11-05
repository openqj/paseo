import { Command } from "commander";
import { hostname } from "node:os";
import { setTimeout as delay } from "node:timers/promises";
import { readDaemonInstance } from "@getpaseo/server/daemon-control";
import { editPersistedConfig } from "@getpaseo/server/configuration";
import {
  ConnectionOfferSchema,
  parseConnectionOfferFromUrl,
} from "@getpaseo/protocol/connection-offer";
import { shouldUseTlsForDefaultHostedRelay } from "@getpaseo/protocol/daemon-endpoints";
import { withOutput, type CommandOptions } from "../../output/index.js";
import { addJsonAndDaemonHostOptions } from "../../utils/command-options.js";
import { runRestartCommand } from "./restart.js";
import { confirmRelayPairing, resolveLocalPairingOffer } from "./pair.js";

interface RegisterOptions extends CommandOptions {
  identityUrl?: string;
}

interface DeviceAuthorizationStart {
  device_code: string;
  user_code: string;
  expires_in: number;
  interval: number;
}

interface DeviceAuthorizationPoll {
  status: "pending" | "slow_down" | "approved" | "denied" | "expired" | "consumed";
  interval: number;
  account_id?: string;
}

function resolveIdentityUrl(value?: string): string {
  const raw = (value ?? process.env.PASEO_IDENTITY_ISSUER ?? "").trim();
  if (!raw)
    throw new Error("Set --identity-url or PASEO_IDENTITY_ISSUER to your Product Identity URL.");
  const url = new URL(raw);
  if (
    !["https:", "http:"].includes(url.protocol) ||
    url.username ||
    url.password ||
    url.search ||
    url.hash
  ) {
    throw new Error(
      "Identity URL must be an HTTPS issuer URL without credentials, query, or fragment.",
    );
  }
  if (url.protocol === "http:" && !["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)) {
    throw new Error("Identity URL must use HTTPS outside localhost.");
  }
  url.pathname = `${url.pathname.replace(/\/+$/, "")}/`;
  return url.href;
}

async function identityRequest<T>(url: string, init: RequestInit): Promise<T> {
  const response = await fetch(url, {
    ...init,
    redirect: "error",
    signal: AbortSignal.timeout(10_000),
  });
  const body = await response.text();
  if (!response.ok || body.length > 32_768) {
    throw new Error(`Identity request failed (HTTP ${response.status}).`);
  }
  try {
    return JSON.parse(body) as T;
  } catch {
    throw new Error("Identity returned an invalid host registration response.");
  }
}

async function startAuthorization(
  identityUrl: string,
  host: {
    serverId: string;
    daemonPublicKeyB64: string;
    relayEndpoint: string;
    relayUseTls: boolean;
  },
): Promise<DeviceAuthorizationStart> {
  const result = await identityRequest<DeviceAuthorizationStart>(
    new URL("v1/auth/paseo/device-authorizations", identityUrl).href,
    {
      method: "POST",
      headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
      body: JSON.stringify({
        server_id: host.serverId,
        host_name: hostname(),
        daemon_public_key_b64: host.daemonPublicKeyB64,
        relay_endpoint: host.relayEndpoint,
        relay_use_tls: host.relayUseTls,
      }),
    },
  );
  if (
    typeof result.device_code !== "string" ||
    result.device_code.length < 32 ||
    typeof result.user_code !== "string" ||
    result.user_code.length !== 8 ||
    !Number.isInteger(result.expires_in) ||
    result.expires_in <= 0 ||
    result.expires_in > 600 ||
    !Number.isInteger(result.interval) ||
    result.interval < 5 ||
    result.interval > 60
  ) {
    throw new Error("Identity returned invalid host registration details.");
  }
  return result;
}

async function pollAuthorization(
  identityUrl: string,
  authorization: DeviceAuthorizationStart,
): Promise<string> {
  const deadline = Date.now() + authorization.expires_in * 1_000;
  let interval = authorization.interval;
  while (Date.now() < deadline) {
    await delay(Math.min(interval * 1_000, Math.max(0, deadline - Date.now())));
    if (Date.now() >= deadline) break;
    const result = await identityRequest<DeviceAuthorizationPoll>(
      new URL("v1/auth/paseo/device-authorizations/token", identityUrl).href,
      {
        method: "POST",
        headers: { "Content-Type": "application/json", "Cache-Control": "no-store" },
        body: JSON.stringify({ device_code: authorization.device_code }),
      },
    );
    if (result.status === "approved") {
      if (typeof result.account_id !== "string" || !result.account_id.startsWith("acct:")) {
        throw new Error("Identity approved the host without returning its account binding.");
      }
      return result.account_id;
    }
    if (result.status === "denied") throw new Error("Workbench denied Paseo host registration.");
    if (result.status === "expired" || result.status === "consumed") {
      throw new Error("Paseo host registration expired or was already completed.");
    }
    if (result.status === "slow_down") interval = Math.max(interval + 5, result.interval);
    else if (Number.isInteger(result.interval) && result.interval >= 5) interval = result.interval;
  }
  throw new Error("Paseo host registration expired before approval.");
}

export function paseoHostRegisterCommand(): Command {
  return addJsonAndDaemonHostOptions(
    new Command("register").description(
      "Register this Paseo host to the signed-in Workbench account",
    ),
  )
    .option("--identity-url <url>", "Product Identity issuer URL (or PASEO_IDENTITY_ISSUER)")
    .action(withOutput(runPaseoHostRegisterCommand));
}

export async function runPaseoHostRegisterCommand(options: RegisterOptions, _command: Command) {
  if (options.daemonTarget.kind !== "instance") {
    throw new Error("Host registration requires a local --home Paseo daemon.");
  }
  const identityUrl = resolveIdentityUrl(options.identityUrl);
  const pairing = await resolveLocalPairingOffer({ paseoHome: options.daemonTarget.home });
  let connectionOffer = pairing.url ? parseConnectionOfferFromUrl(pairing.url) : null;
  if (!pairing.relayEnabled || !connectionOffer) {
    const enableRelay = process.stderr.isTTY === true && (await confirmRelayPairing());
    if (!enableRelay) {
      throw new Error("Enable Paseo Relay before registering this host.");
    }
    const enabledPairing = await resolveLocalPairingOffer({
      paseoHome: options.daemonTarget.home,
      enableRelay: true,
    });
    connectionOffer = enabledPairing.url ? parseConnectionOfferFromUrl(enabledPairing.url) : null;
  }
  const parsedOffer = ConnectionOfferSchema.safeParse(connectionOffer);
  if (!parsedOffer.success) throw new Error("Paseo Relay connection metadata is unavailable.");

  const authorization = await startAuthorization(identityUrl, {
    serverId: parsedOffer.data.serverId,
    daemonPublicKeyB64: parsedOffer.data.daemonPublicKeyB64,
    relayEndpoint: parsedOffer.data.relay.endpoint,
    relayUseTls:
      parsedOffer.data.relay.useTls ??
      shouldUseTlsForDefaultHostedRelay(parsedOffer.data.relay.endpoint),
  });
  process.stderr.write(
    `Paseo host: ${hostname()}\n` +
      `Workbench approval code: ${authorization.user_code}\n` +
      "In Web A, open Workbench, enter this code, review the host details, and approve it.\n",
  );
  const accountId = await pollAuthorization(identityUrl, authorization);
  editPersistedConfig(options.daemonTarget.home, "daemon.auth.identity", {
    value: {
      issuerUrl: identityUrl,
      accountId,
      serverId: parsedOffer.data.serverId,
    },
  });

  const instance = await readDaemonInstance(options.daemonTarget.home);
  if (instance) {
    await runRestartCommand(
      {
        ...options,
        timeout: "600",
      },
      new Command("restart"),
    );
  }
  return {
    type: "single" as const,
    data: {
      hostName: hostname(),
      serverId: parsedOffer.data.serverId,
      accountId,
      status: "registered",
      restarted: Boolean(instance),
    },
    schema: {
      idField: "serverId" as const,
      columns: [],
      renderHuman: () =>
        instance
          ? `Registered ${hostname()} to Workbench and restarted the Paseo daemon.`
          : `Registered ${hostname()} to Workbench. Start the Paseo daemon to connect.`,
    },
  };
}
