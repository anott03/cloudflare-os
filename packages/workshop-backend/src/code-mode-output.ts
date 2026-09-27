import { RpcTarget } from "cloudflare:workers";
import { validateRpc } from "capnweb-validate";
import type { AgentCodeOutput, AgentImageOutput, ChatAttachmentRef } from "@gadgets/workshop-shared/api";
import { inspectAgentPng, MAX_AGENT_IMAGE_BYTES } from "@gadgets/workshop-shared/png";
import { validateChatAttachmentUpload } from "./chat-attachment-validation";

/** A staged image reference with bytes available to the currently executing agent tool. */
export type CodeModeImage = ChatAttachmentRef & { content: Uint8Array };

/** Execution-scoped output capability; it cannot read files, fetch URLs, or publish public assets. */
@validateRpc()
export class CodeModeOutput extends RpcTarget implements AgentCodeOutput {
  #publish: (image: AgentImageOutput) => void;
  #active = true;
  #bytes = 0;
  #count = 0;

  constructor(publish: (image: AgentImageOutput) => void) {
    super();
    this.#publish = publish;
  }

  [Symbol.dispose](): void { this.#active = false; }

  async image(image: AgentImageOutput): Promise<void> {
    if (!this.#active) throw new Error("Image output is only available during its code execution.");
    inspectAgentPng(image.content);
    if (this.#count >= 4 || this.#bytes + image.content.byteLength > MAX_AGENT_IMAGE_BYTES) {
      throw new Error("Emit at most four images and 1 MiB total per code execution.");
    }
    const validated = validateChatAttachmentUpload(image);
    const snapshot: AgentImageOutput = {
      mimeType: "image/png", content: new Uint8Array(validated.content), name: validated.name,
    };
    this.#publish(snapshot);
    this.#count++;
    this.#bytes += snapshot.content.byteLength;
  }
}
