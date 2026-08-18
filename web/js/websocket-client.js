import { decodeFrameMessage, packFrameMessage } from "./protocol.js";
import { normalizeSecret } from "./session-link.js";

const OPEN = 1;
const DEFAULT_BUFFER_THRESHOLD = 524288;

export function connectRelay(url, WebSocketConstructor = globalThis.WebSocket) {
  if (typeof WebSocketConstructor !== "function") {
    throw new Error("WebSocket is unavailable in this browser");
  }
  return new RelaySocket(new WebSocketConstructor(url), url);
}

export class RelaySocket {
  constructor(socket, url) {
    this.socket = socket;
    this.url = url;
    this.onFrame = null;
    this.onControl = null;
    this.onError = null;
    this._joined = false;
    this._joinWaiter = null;
    this._openSettled = false;
    this.open = new Promise((resolve, reject) => {
      this._resolveOpen = resolve;
      this._rejectOpen = reject;
    });
    socket.binaryType = "arraybuffer";
    bindSocketEvent(socket, "open", () => {
      this._openSettled = true;
      this._resolveOpen(this);
    });
    bindSocketEvent(socket, "message", (event) => {
      void this._handleMessage(event);
    });
    bindSocketEvent(socket, "error", (event) => {
      const error = new Error("relay WebSocket error");
      if (!this._openSettled) {
        this._openSettled = true;
        this._rejectOpen(error);
      }
      if (this._joinWaiter) {
        this._joinWaiter.reject(error);
        this._joinWaiter = null;
      }
      this.onError?.(error, event);
    });
    bindSocketEvent(socket, "close", (event) => {
      if (!this._openSettled) {
        this._openSettled = true;
        this._rejectOpen(new Error("relay WebSocket closed before opening"));
      }
      if (this._joinWaiter) {
        this._joinWaiter.reject(new Error("relay WebSocket closed before ready"));
        this._joinWaiter = null;
      }
      this.onControl?.({ type: "close", code: event?.code ?? 1000 });
    });
  }

  async waitUntilOpen() {
    if (this.socket.readyState === OPEN) {
      return this;
    }
    return this.open;
  }

  close(code, reason) {
    this.socket.close(code, reason);
  }

  async _handleMessage(event) {
    const data = event?.data ?? event;
    if (typeof data === "string") {
      try {
        this._handleControl(JSON.parse(data));
      } catch (error) {
        this.onError?.(error);
      }
      return;
    }
    if (data && typeof data.arrayBuffer === "function") {
      try {
        const bytes = await data.arrayBuffer();
        this.onFrame?.(decodeFrameMessage(bytes));
      } catch (error) {
        this.onError?.(error);
      }
      return;
    }
    try {
      const frame = decodeFrameMessage(data);
      this.onFrame?.(frame);
    } catch (error) {
      this.onError?.(error);
    }
  }

  _handleControl(message) {
    if (message?.type === "ready" && this._joinWaiter) {
      this._joined = true;
      this._joinWaiter.resolve(message);
      this._joinWaiter = null;
      return;
    }
    if (message?.type === "error" && this._joinWaiter) {
      const error = new RelayError(message.code, message.message);
      this._joinWaiter.reject(error);
      this._joinWaiter = null;
      return;
    }
    this.onControl?.(message);
  }
}

export async function sendJoin(socket, secret) {
  const relay = asRelaySocket(socket);
  normalizeSecret(secret);
  await relay.waitUntilOpen();
  if (relay._joined) {
    throw new Error("relay socket has already joined");
  }
  if (relay._joinWaiter) {
    throw new Error("relay join is already pending");
  }
  return new Promise((resolve, reject) => {
    relay._joinWaiter = { resolve, reject };
    try {
      relay.socket.send(JSON.stringify({ type: "join", token: secret }));
    } catch (error) {
      relay._joinWaiter = null;
      reject(error);
    }
  });
}

export function sendFrame(socket, frame, threshold = DEFAULT_BUFFER_THRESHOLD) {
  const relay = asRelaySocket(socket);
  if (relay.socket.readyState !== OPEN) {
    return { sent: false, skipped: true, reason: "SOCKET_NOT_OPEN" };
  }
  if (isSocketBackpressured(relay, threshold)) {
    return { sent: false, skipped: true, reason: "BUFFERED_AMOUNT_HIGH" };
  }
  const payload = frame.payload;
  const message = packFrameMessage(frame.meta, payload);
  relay.socket.send(message);
  return { sent: true, skipped: false, bytesWritten: message.byteLength };
}

export function isSocketBackpressured(socket, threshold = DEFAULT_BUFFER_THRESHOLD) {
  const amount = Number(socket?.bufferedAmount ?? socket?.socket?.bufferedAmount ?? 0);
  const selectedThreshold = Number.isInteger(threshold) && threshold >= 0
    ? threshold
    : DEFAULT_BUFFER_THRESHOLD;
  return amount > selectedThreshold;
}

export class RelayError extends Error {
  constructor(code, message) {
    super(message || code || "relay error");
    this.name = "RelayError";
    this.code = code;
  }
}

function asRelaySocket(socket) {
  if (socket instanceof RelaySocket) {
    return socket;
  }
  if (socket?.socket instanceof RelaySocket) {
    return socket.socket;
  }
  if (socket?.send && socket?.readyState !== undefined) {
    return { socket };
  }
  throw new TypeError("socket must be a RelaySocket or WebSocket");
}

function bindSocketEvent(socket, event, handler) {
  if (typeof socket.addEventListener === "function") {
    socket.addEventListener(event, handler);
    return;
  }
  socket[`on${event}`] = handler;
}
