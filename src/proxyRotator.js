const https = require('https');
const net = require('net');
const tls = require('tls');

class ProxyRotator {
  constructor(apiKey = null) {
    this.apiKey = apiKey;
    this.proxies = [];
    this.currentIndex = 0;
    this.lastFetchTime = 0;
    this.fetchInterval = 10 * 60 * 1000; // Fetch every 10 minutes
    this.isFetching = false;
  }

  async initialize() {
    if (this.apiKey) {
      await this.fetchProxies().catch(err => {
        console.error(`[PROXY-ROTATOR] Initial fetch failed: ${err.message}`);
      });
    }
  }

  hasProxies() {
    return this.proxies.length > 0;
  }

  async getNextProxy() {
    if (!this.apiKey) return null;
    
    // Auto-refresh proxies if they are older than fetchInterval
    if (Date.now() - this.lastFetchTime > this.fetchInterval) {
      await this.fetchProxies().catch(err => {
        console.error(`[PROXY-ROTATOR] Auto-refresh failed: ${err.message}`);
      });
    }

    if (this.proxies.length === 0) return null;
    
    const proxy = this.proxies[this.currentIndex];
    this.currentIndex = (this.currentIndex + 1) % this.proxies.length;
    return proxy;
  }

  async fetchProxies() {
    if (this.isFetching) return;
    this.isFetching = true;
    console.log('[PROXY-ROTATOR] Fetching proxies from Webshare...');
    try {
      const results = await this._fetchFromWebshare();
      this.proxies = results.filter(p => p.valid);
      this.lastFetchTime = Date.now();
      this.currentIndex = 0;
      console.log(`[PROXY-ROTATOR] Successfully loaded ${this.proxies.length} valid proxies from Webshare`);
    } catch (error) {
      console.error(`[PROXY-ROTATOR] Failed to fetch proxies: ${error.message}`);
      throw error;
    } finally {
      this.isFetching = false;
    }
  }

  _fetchFromWebshare() {
    return new Promise((resolve, reject) => {
      const url = 'https://proxy.webshare.io/api/v2/proxy/list/?mode=direct&page=1&page_size=25';
      const options = {
        headers: {
          'Authorization': `Token ${this.apiKey}`
        },
        timeout: 10000
      };
      
      const req = https.get(url, options, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => {
          if (res.statusCode >= 400) {
            reject(new Error(`Webshare API returned status ${res.statusCode}`));
            return;
          }
          try {
            const json = JSON.parse(data);
            resolve(json.results || []);
          } catch (e) {
            reject(new Error('Failed to parse Webshare JSON response'));
          }
        });
      });

      req.on('error', reject);
      req.on('timeout', () => {
        req.destroy();
        reject(new Error('Webshare API request timed out'));
      });
    });
  }

  /**
   * Creates an upgraded TLS socket tunnelled through the specified HTTP proxy
   */
  createConnection(proxy, targetHost, targetPort = 443) {
    return new Promise((resolve, reject) => {
      const maskedProxy = `${proxy.proxy_address}:${proxy.port}`;
      console.log(`[PROXY-TUNNEL] Creating tunnel via ${maskedProxy} to ${targetHost}:${targetPort}`);
      
      const socket = net.connect(proxy.port, proxy.proxy_address);
      socket.setTimeout(15000); // 15s timeout for connection
      
      socket.on('error', (err) => {
        socket.destroy();
        reject(new Error(`Proxy TCP connection failed: ${err.message}`));
      });

      socket.on('timeout', () => {
        socket.destroy();
        reject(new Error('Proxy TCP connection timed out'));
      });

      socket.on('connect', () => {
        const auth = Buffer.from(`${proxy.username}:${proxy.password}`).toString('base64');
        const connectRequest = [
          `CONNECT ${targetHost}:${targetPort} HTTP/1.1`,
          `Host: ${targetHost}:${targetPort}`,
          `Proxy-Authorization: Basic ${auth}`,
          '',
          ''
        ].join('\r\n');

        socket.write(connectRequest);
      });

      let buffer = '';
      const onData = (chunk) => {
        buffer += chunk.toString();
        const headerEnd = buffer.indexOf('\r\n\r\n');
        if (headerEnd !== -1) {
          socket.off('data', onData);
          socket.setTimeout(0); // clear timeout
          
          const firstLine = buffer.substring(0, buffer.indexOf('\r\n'));
          if (firstLine.includes('200')) {
            try {
              const secureSocket = tls.connect({
                socket: socket,
                servername: targetHost,
                rejectUnauthorized: true
              });
              secureSocket.on('error', (err) => {
                secureSocket.destroy();
                reject(new Error(`TLS handshake over proxy failed: ${err.message}`));
              });
              resolve(secureSocket);
            } catch (err) {
              reject(err);
            }
          } else {
            socket.destroy();
            reject(new Error(`Proxy CONNECT tunnel handshake failed: ${firstLine}`));
          }
        }
      };
      socket.on('data', onData);
    });
  }
}

module.exports = ProxyRotator;
