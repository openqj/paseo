import { compare } from "bcryptjs";
import type { WSHelloMessage } from "@getpaseo/protocol/messages";
import { OWNER_PERMISSIONS } from "./authorization/index.js";
import { matchesLocalCredential } from "./local-credential.js";
import { resolveIdentityTokenAccountId } from "./identity-token-auth.js";
import type { SessionAdmission } from "./websocket-server.js";

export type AdmissionFailure = "password_required" | "incorrect_password";

type AdmissionResolution = { admission: SessionAdmission } | { rejection: AdmissionFailure };

export async function resolveSessionAdmission(input: {
  credential: WSHelloMessage["auth"];
  passwordHash: string | undefined;
  identity?: { issuerUrl: string; accountId: string; serverId?: string };
  localCredential: string | null;
  transport: "direct" | "relay";
  fetchIdentityAccountId?: (issuerUrl: string, token: string) => Promise<string | null>;
}): Promise<AdmissionResolution> {
  const { credential, passwordHash, identity, localCredential, transport } = input;
  if (!passwordHash && !identity) {
    return { admission: { principalId: "owner", permissions: OWNER_PERMISSIONS } };
  }
  if (!credential) {
    if (identity) return { rejection: "password_required" };
    // COMPAT(relayPasswordOptional): added in v0.9.1, remove once release N mobile builds are live on App Store and Play.
    if (transport === "relay") {
      return { admission: { principalId: "owner", permissions: OWNER_PERMISSIONS } };
    }
    return { rejection: "password_required" };
  }
  if (credential.kind === "localCredential") {
    if (localCredential && matchesLocalCredential(localCredential, credential.token)) {
      return { admission: { principalId: "owner", permissions: OWNER_PERMISSIONS } };
    }
    return { rejection: "incorrect_password" };
  }
  if (credential.kind === "identityToken") {
    if (!identity || !credential.token) return { rejection: "incorrect_password" };
    try {
      const accountId = await (input.fetchIdentityAccountId ?? resolveIdentityTokenAccountId)(
        identity.issuerUrl,
        credential.token,
        identity.serverId,
      );
      return accountId === identity.accountId
        ? { admission: { principalId: `identity:${accountId}`, permissions: OWNER_PERMISSIONS } }
        : { rejection: "incorrect_password" };
    } catch {
      return { rejection: "incorrect_password" };
    }
  }
  if (!passwordHash) return { rejection: "incorrect_password" };
  return (await compare(credential.password, passwordHash))
    ? { admission: { principalId: "owner", permissions: OWNER_PERMISSIONS } }
    : { rejection: "incorrect_password" };
}
