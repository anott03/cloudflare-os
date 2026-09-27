/** Maximum encoded PNG size accepted for agent image output. */
export const MAX_AGENT_IMAGE_BYTES = 1024 * 1024;

/** Maximum decoded pixel count accepted for agent image output. */
export const MAX_AGENT_IMAGE_PIXELS = 8 * 1024 * 1024;

/** Reject oversized or mislabeled PNGs and return their declared dimensions, without decoding pixels. */
export function inspectAgentPng(content: Uint8Array): { width: number; height: number } {
  if (content.byteLength < 33 || content.byteLength > MAX_AGENT_IMAGE_BYTES) {
    throw new Error("PNG must contain between 33 bytes and 1 MiB.");
  }
  const signature = [137, 80, 78, 71, 13, 10, 26, 10];
  if (!signature.every((value, index) => content[index] === value)) throw new Error("Expected PNG image data.");
  const view = new DataView(content.buffer, content.byteOffset, content.byteLength);
  if (view.getUint32(8) !== 13 || view.getUint32(12) !== 0x49484452) throw new Error("Invalid PNG header.");
  const width = view.getUint32(16);
  const height = view.getUint32(20);
  if (width === 0 || height === 0 || width > 4096 || height > 8192 || width * height > MAX_AGENT_IMAGE_PIXELS) {
    throw new Error("PNG exceeds the image dimension limit.");
  }
  return { width, height };
}
