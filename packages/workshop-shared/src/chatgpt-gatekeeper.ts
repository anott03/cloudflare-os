// ChatGPT-gatekeeper-specific extension to the generic GatekeeperUser contract.
//
// The ChatGPT gatekeeper holds a "Sign in with ChatGPT" grant, whose access token the OpenAI
// Responses API accepts in place of an API key and bills to the user's ChatGPT subscription. The
// Workshop holds the GatekeeperUser stub and narrows it to this interface to read the token for the
// models of provider "chatgpt".

import { GatekeeperUser } from "./gatekeeper.js";

/** Vendor id of the ChatGPT gatekeeper: the suffix of its GATEKEEPER_CHATGPT binding, lowercased. */
export const CHATGPT_VENDOR_ID = "chatgpt";

export interface ChatGptGatekeeperUser extends GatekeeperUser {
  /**
   * Returns a currently-usable (refreshed if needed) Sign in with ChatGPT access token, or null if
   * the connection is broken or expired. Workshop-only: never exposed to gadgets or agents.
   */
  getUsableAccessToken(): Promise<string | null>;
}
