const https = require('https');
const { URL } = require('url');

class OpenAIClient {
  constructor(keyRotator, baseUrl = 'https://api.openai.com', proxyRotator = null) {
    this.keyRotator = keyRotator;
    this.baseUrl = baseUrl;
    this.proxyRotator = proxyRotator;
  }

  async makeRequest(method, path, body, headers = {}, customStatusCodes = null, streaming = false) {
    // Create a new request context for this specific request
    const requestContext = this.keyRotator.createRequestContext();
    let lastError = null;
    let lastResponse = null;
    const failedKeys = []; // Track which keys failed and why

    // Determine which status codes should trigger rotation
    const rotationStatusCodes = customStatusCodes || new Set([400, 429, 404, 500, 502, 503, 504]);

    // Try each available key for this request
    let apiKey;
    while ((apiKey = requestContext.getNextKey()) !== null) {
      const maskedKey = this.maskApiKey(apiKey);

      // Determine how many proxy attempts we will make for this key
      const hasProxies = this.proxyRotator && this.proxyRotator.hasProxies();
      const maxProxyAttempts = hasProxies ? 2 : 1;

      for (let proxyAttempt = 0; proxyAttempt < maxProxyAttempts; proxyAttempt++) {
        const proxy = hasProxies ? await this.proxyRotator.getNextProxy() : null;
        const proxyLog = proxy ? ` via proxy ${proxy.proxy_address}:${proxy.port} (attempt ${proxyAttempt + 1}/2)` : '';

        console.log(`[OPENAI::${maskedKey}] Attempting ${method} ${path}${streaming ? ' (streaming)' : ''}${proxyLog}`);

        try {
          if (streaming) {
            const response = await this.sendStreamingRequest(method, path, body, headers, apiKey, proxy);

            if (rotationStatusCodes.has(response.statusCode)) {
              if (hasProxies && proxyAttempt < maxProxyAttempts - 1) {
                console.log(`[OPENAI::${maskedKey}] Status ${response.statusCode} - assuming proxy issue, rotating proxy...`);
                response.stream.resume();
                lastResponse = { statusCode: response.statusCode, headers: response.headers, data: '' };
                continue;
              }

              console.log(`[OPENAI::${maskedKey}] Status ${response.statusCode} triggers rotation - trying next key`);
              response.stream.resume();
              requestContext.markKeyAsRateLimited(apiKey);
              failedKeys.push({ key: maskedKey, status: response.statusCode, reason: 'rate_limited' });
              lastResponse = { statusCode: response.statusCode, headers: response.headers, data: '' };
              break; // Break proxy loop to move to next key
            }

            console.log(`[OPENAI::${maskedKey}] Success (${response.statusCode}) - streaming`);
            this.keyRotator.incrementKeyUsage(apiKey);
            response._keyInfo = { keyUsed: maskedKey, failedKeys };
            return response;
          } else {
            const response = await this.sendRequest(method, path, body, headers, apiKey, proxy);

            if (rotationStatusCodes.has(response.statusCode)) {
              if (hasProxies && proxyAttempt < maxProxyAttempts - 1) {
                console.log(`[OPENAI::${maskedKey}] Status ${response.statusCode} - assuming proxy issue, rotating proxy...`);
                lastResponse = response;
                continue;
              }

              console.log(`[OPENAI::${maskedKey}] Status ${response.statusCode} triggers rotation - trying next key`);
              requestContext.markKeyAsRateLimited(apiKey);
              failedKeys.push({ key: maskedKey, status: response.statusCode, reason: 'rate_limited' });
              lastResponse = response;
              break; // Break proxy loop to move to next key
            }

            console.log(`[OPENAI::${maskedKey}] Success (${response.statusCode})`);
            this.keyRotator.incrementKeyUsage(apiKey);
            response._keyInfo = { keyUsed: maskedKey, failedKeys };
            return response;
          }
        } catch (error) {
          console.log(`[OPENAI::${maskedKey}] Request failed: ${error.message}`);

          if (hasProxies && proxyAttempt < maxProxyAttempts - 1) {
            console.log(`[OPENAI::${maskedKey}] Request error - assuming proxy issue, rotating proxy...`);
            lastError = error;
            continue;
          }

          failedKeys.push({ key: maskedKey, status: null, reason: error.message });
          lastError = error;
          break; // Break proxy loop to move to next key
        }
      }
    }

    // All keys have been tried for this request
    const stats = requestContext.getStats();
    console.log(`[OPENAI] All ${stats.totalKeys} keys tried for this request. ${stats.rateLimitedKeys} were rate limited.`);

    const lastFailedKey = requestContext.getLastFailedKey();
    this.keyRotator.updateLastFailedKey(lastFailedKey);

    if (requestContext.allTriedKeysRateLimited()) {
      console.log('[OPENAI] All keys rate limited for this request - returning 429');
      const response = lastResponse || {
        statusCode: 429,
        headers: { 'content-type': 'application/json' },
        data: JSON.stringify({
          error: {
            message: 'All OpenAI API keys have been rate limited for this request',
            type: 'rate_limit_exceeded',
            code: 'rate_limit_exceeded'
          }
        })
      };
      response._keyInfo = { keyUsed: null, failedKeys };
      return response;
    }

    if (lastError) {
      throw lastError;
    }

    throw new Error('All API keys exhausted without clear error');
  }

  _buildRequestOptions(method, path, body, headers, apiKey) {
    let fullUrl;
    if (!path || path === '/') {
      fullUrl = this.baseUrl;
    } else if (path.startsWith('/')) {
      fullUrl = this.baseUrl.endsWith('/') ? this.baseUrl + path.substring(1) : this.baseUrl + path;
    } else {
      fullUrl = this.baseUrl.endsWith('/') ? this.baseUrl + path : this.baseUrl + '/' + path;
    }

    const url = new URL(fullUrl);

    const finalHeaders = {
      'Content-Type': 'application/json',
      ...headers
    };

    if (!headers || !headers.authorization) {
      finalHeaders['Authorization'] = `Bearer ${apiKey}`;
    }

    const options = {
      hostname: url.hostname,
      port: url.port || 443,
      path: url.pathname + url.search,
      method: method,
      headers: finalHeaders
    };

    if (body && method !== 'GET') {
      const bodyData = typeof body === 'string' ? body : JSON.stringify(body);
      options.headers['Content-Length'] = Buffer.byteLength(bodyData);
    }

    return options;
  }

  sendRequest(method, path, body, headers, apiKey, proxy = null) {
    return new Promise((resolve, reject) => {
      const options = this._buildRequestOptions(method, path, body, headers, apiKey);

      const makeHttpCall = () => {
        const req = https.request(options, (res) => {
          let data = '';

          res.on('data', (chunk) => {
            data += chunk;
          });

          res.on('end', () => {
            resolve({
              statusCode: res.statusCode,
              headers: res.headers,
              data: data
            });
          });
        });

        req.on('error', (error) => {
          const maskedKey = this.maskApiKey(apiKey);
          console.log(`[OPENAI::${maskedKey}] HTTP request error: ${error.message}`);
          reject(error);
        });

        if (body && method !== 'GET') {
          const bodyData = typeof body === 'string' ? body : JSON.stringify(body);
          req.write(bodyData);
        }

        req.end();
      };

      if (proxy && this.proxyRotator) {
        this.proxyRotator.createConnection(proxy, options.hostname, options.port || 443)
          .then((secureSocket) => {
            options.createConnection = () => secureSocket;
            makeHttpCall();
          })
          .catch((err) => {
            const maskedKey = this.maskApiKey(apiKey);
            console.log(`[OPENAI::${maskedKey}] Proxy connection error: ${err.message}`);
            reject(err);
          });
      } else {
        makeHttpCall();
      }
    });
  }

  sendStreamingRequest(method, path, body, headers, apiKey, proxy = null) {
    return new Promise((resolve, reject) => {
      const options = this._buildRequestOptions(method, path, body, headers, apiKey);

      const makeHttpCall = () => {
        const req = https.request(options, (res) => {
          // Resolve immediately with the raw stream - don't buffer
          resolve({
            statusCode: res.statusCode,
            headers: res.headers,
            stream: res
          });
        });

        req.on('error', (error) => {
          const maskedKey = this.maskApiKey(apiKey);
          console.log(`[OPENAI::${maskedKey}] HTTP streaming request error: ${error.message}`);
          reject(error);
        });

        if (body && method !== 'GET') {
          const bodyData = typeof body === 'string' ? body : JSON.stringify(body);
          req.write(bodyData);
        }

        req.end();
      };

      if (proxy && this.proxyRotator) {
        this.proxyRotator.createConnection(proxy, options.hostname, options.port || 443)
          .then((secureSocket) => {
            options.createConnection = () => secureSocket;
            makeHttpCall();
          })
          .catch((err) => {
            const maskedKey = this.maskApiKey(apiKey);
            console.log(`[OPENAI::${maskedKey}] Proxy streaming connection error: ${err.message}`);
            reject(err);
          });
      } else {
        makeHttpCall();
      }
    });
  }

  maskApiKey(key) {
    if (!key || key.length < 8) return '***';
    return key.substring(0, 4) + '...' + key.substring(key.length - 4);
  }
}

module.exports = OpenAIClient;