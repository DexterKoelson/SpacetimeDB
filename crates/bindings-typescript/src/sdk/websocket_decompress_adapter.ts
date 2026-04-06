import { decompress } from './decompress';
import { resolveWS } from './ws';

export type WebSocketCloseEvent = {
  code: number;
  reason: string;
  wasClean: boolean;
};

export interface WebsocketAdapter {
  send(msg: any): void;
  close(): void;
  onclose?: ((event: any) => void) | null;
  onopen?: ((...ev: any[]) => void) | null;
  onmessage?: ((msg: { data: Uint8Array }) => void) | null;
  onerror?: ((msg: any) => void) | null;
}

export class WebsocketDecompressAdapter implements WebsocketAdapter {
  onclose?: (event: WebSocketCloseEvent) => void;
  onopen?: (...ev: any[]) => void;
  onmessage?: (msg: { data: Uint8Array }) => void;
  onerror?: (msg: ErrorEvent) => void;

  #ws: WebSocket;

  async #decompress(buffer: Uint8Array): Promise<Uint8Array> {
    const tag = buffer[0];
    const data = buffer.subarray(1);
    switch (tag) {
      case 0:
        return data;
      case 1:
        throw new Error(
          'Brotli Compression not supported. Please use gzip or none compression in withCompression method on DbConnection.'
        );
      case 2:
        return await decompress(data, 'gzip');
      default:
        throw new Error(
          'Unexpected Compression Algorithm. Please use `gzip` or `none`'
        );
    }
  }

  #handleOnOpen(msg: any) {
    this.onopen?.(msg);
  }

  #handleOnError(msg: any) {
    this.onerror?.(msg);
  }

  #handleOnClose(msg: any) {
    this.onclose?.({
      code: msg?.code ?? 1006,
      reason: msg?.reason ?? '',
      wasClean: msg?.wasClean ?? false,
    });
  }

  send(msg: any): void {
    this.#ws.send(msg);
  }

  close(): void {
    this.#ws.close();
  }

  constructor(ws: WebSocket) {
    ws.binaryType = 'arraybuffer';

    this.#ws = ws;

    ws.onopen = this.#handleOnOpen.bind(this);
    ws.onerror = this.#handleOnError.bind(this);
    ws.onclose = this.#handleOnClose.bind(this);
    ws.onmessage = async (msg: MessageEvent<ArrayBuffer>) => {
      const data = await this.#decompress(new Uint8Array(msg.data));
      this.onmessage?.({ data });
    };
  }

  static async createWebSocketFn({
    url,
    nameOrAddress,
    wsProtocol,
    authToken,
    compression,
    lightMode,
    confirmedReads,
  }: {
    url: URL;
    wsProtocol: string;
    nameOrAddress: string;
    authToken?: string;
    compression: 'gzip' | 'none';
    lightMode: boolean;
    confirmedReads?: boolean;
  }): Promise<WebsocketDecompressAdapter> {
    const headers = new Headers();

    const WS = await resolveWS();

    // We swap our original token to a shorter-lived token
    // to avoid sending the original via query params.
    let temporaryAuthToken: string | undefined = undefined;
    if (authToken) {
      headers.set('Authorization', `Bearer ${authToken}`);
      const tokenUrl = new URL('v1/identity/websocket-token', url);
      tokenUrl.protocol = url.protocol === 'wss:' ? 'https:' : 'http:';

      const response = await fetch(tokenUrl, { method: 'POST', headers });
      if (response.ok) {
        const { token } = await response.json();
        temporaryAuthToken = token;
      } else {
        return Promise.reject(
          new Error(`Failed to verify token: ${response.statusText}`)
        );
      }
    }

    const databaseUrl = new URL(`v1/database/${nameOrAddress}/subscribe`, url);
    if (temporaryAuthToken) {
      databaseUrl.searchParams.set('token', temporaryAuthToken);
    }
    databaseUrl.searchParams.set(
      'compression',
      compression === 'gzip' ? 'Gzip' : 'None'
    );
    if (lightMode) {
      databaseUrl.searchParams.set('light', 'true');
    }
    if (confirmedReads !== undefined) {
      databaseUrl.searchParams.set('confirmed', confirmedReads.toString());
    }

    const ws = new WS(databaseUrl.toString(), wsProtocol);

    return new WebsocketDecompressAdapter(ws);
  }
}
