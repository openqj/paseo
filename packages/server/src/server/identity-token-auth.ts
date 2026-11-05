export async function resolveIdentityTokenAccountId(
  issuerUrl: string,
  token: string,
  serverId?: string,
): Promise<string | null> {
  const issuerBaseUrl = issuerUrl.endsWith("/") ? issuerUrl : `${issuerUrl}/`;
  const path = serverId
    ? `v1/auth/paseo/hosts/${encodeURIComponent(serverId)}/authorize`
    : "v1/auth/me";
  const response = await fetch(new URL(path, issuerBaseUrl), {
    method: "GET",
    headers: { Authorization: `Bearer ${token}`, Accept: "application/json" },
    redirect: "error",
    signal: AbortSignal.timeout(5_000),
  });
  if (!response.ok) return null;
  const body = await response.text();
  if (body.length > 32_768) return null;
  try {
    const accountId = (JSON.parse(body) as { account_id?: unknown }).account_id;
    return typeof accountId === "string" && accountId.length <= 128 ? accountId : null;
  } catch {
    return null;
  }
}
