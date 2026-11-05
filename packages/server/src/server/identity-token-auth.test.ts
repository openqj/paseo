import { afterEach, describe, expect, test, vi } from "vitest";
import { resolveIdentityTokenAccountId } from "./identity-token-auth.js";

describe("Identity token account lookup", () => {
  afterEach(() => vi.unstubAllGlobals());

  test("preserves a path prefix in the configured issuer URL", async () => {
    let requestedUrl: URL | undefined;
    vi.stubGlobal(
      "fetch",
      async (input: URL | RequestInfo) => {
        requestedUrl = new URL(input.toString());
        return new Response(JSON.stringify({ account_id: "acct:test" }), { status: 200 });
      },
    );

    await expect(
      resolveIdentityTokenAccountId("https://identity.example.test/identity", "token"),
    ).resolves.toBe("acct:test");
    expect(requestedUrl?.pathname).toBe("/identity/v1/auth/me");
  });
});
