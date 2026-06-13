const https = require('https');
const { URL } = require('url');

class GeminiClient {
  constructor(keyRotator, baseUrl = 'https://generativelanguage.googleapis.com') {
    this.keyRotator = keyRotator;
    this.baseUrl = baseUrl;
  }

  async makeRequest(method, path, body, headers = {}, customStatusCodes = null, streaming = false) {
    // Check if an API key was provided in headers
    const providedApiKey = headers['x-goog-api-key'];

    // If an API key was provided, use it directly without rotation
    if (providedApiKey) {
      const maskedKey = this.maskApiKey(providedApiKey);
      console.log(`[GEMINI::${maskedKey}] Using provided API key${streaming ? ' (streaming)' : ''}`);

      const cleanHeaders = { ...headers };
      delete cleanHeaders['x-goog-api-key'];

      try {
        if (streaming) {
          const response = await this.sendStreamingRequest(method, path, body, cleanHeaders, providedApiKey, true);
          console.log(`[GEMINI::${maskedKey}] Response (${response.statusCode}) - streaming`);
          response._keyInfo = { keyUsed: maskedKey, failedKeys: [] };
          return response;
        } else {
          const response = await this.sendRequest(method, path, body, cleanHeaders, providedApiKey, true);
          console.log(`[GEMINI::${maskedKey}] Response (${response.statusCode})`);
          response._keyInfo = { keyUsed: maskedKey, failedKeys: [] };
          return response;
        }
      } catch (error) {
        console.log(`[GEMINI::${maskedKey}] Request failed: ${error.message}`);
        throw error;
      }
    }

    // No API key provided, use rotation system
    const requestContext = this.keyRotator.createRequestContext();
    let lastError = null;
    let lastResponse = null;
    const failedKeys = [];

    const rotationStatusCodes = customStatusCodes || new Set([429]);

    let apiKey;
    while ((apiKey = requestContext.getNextKey()) !== null) {
      const maskedKey = this.maskApiKey(apiKey);

      console.log(`[GEMINI::${maskedKey}] Attempting ${method} ${path}${streaming ? ' (streaming)' : ''}`);

      try {
        if (streaming) {
          const response = await this.sendStreamingRequest(method, path, body, headers, apiKey, false);

          // If the streaming request returned an error status code, drain the stream to get the error payload
          if (response.statusCode >= 400) {
            response.data = await this.drainStreamingResponse(response.stream);
          }

          const rotationReason = this.getRotationReason(response, rotationStatusCodes);
          if (rotationReason) {
            console.log(`[GEMINI::${maskedKey}] ${rotationReason.logMessage} - trying next key`);
            const isRateLimited = rotationReason.reason === 'rate_limited';
            if (isRateLimited) {
              requestContext.markKeyAsRateLimited(apiKey);
            } else {
              requestContext.markKeyAsFailed(apiKey);
            }
            failedKeys.push({ key: maskedKey, status: response.statusCode, reason: rotationReason.reason });
            lastResponse = { statusCode: response.statusCode, headers: response.headers, data: response.data };
            continue;
          }

          // If we have an error status code but it did NOT trigger rotation, return the error response directly
          if (response.statusCode >= 400) {
            console.log(`[GEMINI::${maskedKey}] Request failed with status ${response.statusCode} - not rotating`);
            response._keyInfo = { keyUsed: maskedKey, failedKeys };
            return response;
          }

          console.log(`[GEMINI::${maskedKey}] Success (${response.statusCode}) - streaming`);
          this.keyRotator.incrementKeyUsage(apiKey);
          response._keyInfo = { keyUsed: maskedKey, failedKeys };
          return response;
        } else {
          const response = await this.sendRequest(method, path, body, headers, apiKey, false);

          const rotationReason = this.getRotationReason(response, rotationStatusCodes);
          if (rotationReason) {
            console.log(`[GEMINI::${maskedKey}] ${rotationReason.logMessage} - trying next key`);
            if (rotationReason.reason === 'rate_limited') {
              requestContext.markKeyAsRateLimited(apiKey);
            } else {
              requestContext.markKeyAsFailed(apiKey);
            }
            failedKeys.push({ key: maskedKey, status: response.statusCode, reason: rotationReason.reason });
            lastResponse = response;
            continue;
          }

          console.log(`[GEMINI::${maskedKey}] Success (${response.statusCode})`);
          this.keyRotator.incrementKeyUsage(apiKey);
          response._keyInfo = { keyUsed: maskedKey, failedKeys };
          return response;
        }
      } catch (error) {
        console.log(`[GEMINI::${maskedKey}] Request failed: ${error.message}`);
        failedKeys.push({ key: maskedKey, status: null, reason: error.message });
        lastError = error;
        continue;
      }
    }

    const stats = requestContext.getStats();
    console.log(`[GEMINI] All ${stats.totalKeys} keys tried for this request. ${stats.rateLimitedKeys} were rate limited, ${stats.failedKeys} failed.`);

    const lastFailedKey = requestContext.getLastFailedKey();
    this.keyRotator.updateLastFailedKey(lastFailedKey);

    if (requestContext.allTriedKeysRateLimited()) {
      console.log('[GEMINI] All keys rate limited for this request - returning 429');
      const response = lastResponse || {
        statusCode: 429,
        headers: { 'content-type': 'application/json' },
        data: JSON.stringify({
          error: {
            code: 429,
            message: 'All API keys have been rate limited for this request',
            status: 'RESOURCE_EXHAUSTED'
          }
        })
      };
      response._keyInfo = { keyUsed: null, failedKeys };
      return response;
    }

    if (requestContext.allTriedKeysFailed()) {
      console.log('[GEMINI] All keys failed for this request - returning last error response');
      const response = lastResponse || {
        statusCode: 500,
        headers: { 'content-type': 'application/json' },
        data: JSON.stringify({
          error: {
            code: 500,
            message: 'All API keys failed for this request',
            status: 'INTERNAL'
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

  _buildRequestOptions(method, path, body, headers, apiKey, useHeader) {
    let fullUrl;
    if (!path || path === '/') {
      fullUrl = this.baseUrl;
    } else if (path.startsWith('/')) {
      let effectiveBaseUrl = this.baseUrl;

      const pathVersionMatch = path.match(/^\/v[^\/]+\//);
      const baseVersionMatch = this.baseUrl.match(/\/v[^\/]+$/);

      if (pathVersionMatch && baseVersionMatch) {
        const pathVersion = pathVersionMatch[0].slice(0, -1);
        const baseVersion = baseVersionMatch[0];

        if (pathVersion !== baseVersion) {
          effectiveBaseUrl = this.baseUrl.replace(baseVersion, pathVersion);
          path = path.substring(pathVersion.length);
        }
      }

      fullUrl = effectiveBaseUrl.endsWith('/') ? effectiveBaseUrl + path.substring(1) : effectiveBaseUrl + path;
    } else {
      fullUrl = this.baseUrl.endsWith('/') ? this.baseUrl + path : this.baseUrl + '/' + path;
    }

    const url = new URL(fullUrl);
    this.normalizeOpenAICompatibleUrl(url);

    const finalHeaders = {
      'Content-Type': 'application/json',
      ...headers
    };

    const usesOpenAICompatibility = this.isOpenAICompatibleEndpoint(url.pathname);

    if (usesOpenAICompatibility) {
      if (!headers || !headers.authorization) {
        finalHeaders['Authorization'] = `Bearer ${apiKey}`;
      }
    } else if (useHeader) {
      finalHeaders['x-goog-api-key'] = apiKey;
    } else {
      url.searchParams.append('key', apiKey);
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

  sendRequest(method, path, body, headers, apiKey, useHeader = false) {
    return new Promise((resolve, reject) => {
      const options = this._buildRequestOptions(method, path, body, headers, apiKey, useHeader);

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
        console.log(`[GEMINI::${maskedKey}] HTTP request error: ${error.message}`);
        reject(error);
      });

      if (body && method !== 'GET') {
        const bodyData = typeof body === 'string' ? body : JSON.stringify(body);
        req.write(bodyData);
      }

      req.end();
    });
  }

  sendStreamingRequest(method, path, body, headers, apiKey, useHeader = false) {
    return new Promise((resolve, reject) => {
      const options = this._buildRequestOptions(method, path, body, headers, apiKey, useHeader);

      const req = https.request(options, (res) => {
        resolve({
          statusCode: res.statusCode,
          headers: res.headers,
          stream: res
        });
      });

      req.on('error', (error) => {
        const maskedKey = this.maskApiKey(apiKey);
        console.log(`[GEMINI::${maskedKey}] HTTP streaming request error: ${error.message}`);
        reject(error);
      });

      if (body && method !== 'GET') {
        const bodyData = typeof body === 'string' ? body : JSON.stringify(body);
        req.write(bodyData);
      }

      req.end();
    });
  }

  drainStreamingResponse(stream) {
    return new Promise((resolve) => {
      let data = '';

      stream.setEncoding('utf8');
      stream.on('data', (chunk) => {
        data += chunk;
      });
      stream.on('end', () => {
        resolve(data);
      });
      stream.on('error', () => {
        resolve(data);
      });
    });
  }

  getRotationReason(response, rotationStatusCodes) {
    if (rotationStatusCodes.has(response.statusCode)) {
      return {
        reason: response.statusCode === 429 ? 'rate_limited' : `status_${response.statusCode}`,
        logMessage: `Status ${response.statusCode} triggers rotation`
      };
    }

    const authError = this.getGeminiAuthError(response);
    if (authError) {
      return {
        reason: 'invalid_api_key',
        logMessage: `Gemini auth error (${authError}) triggers rotation`
      };
    }

    return null;
  }

  getGeminiAuthError(response) {
    if (!response || (response.statusCode !== 400 && response.statusCode !== 403) || !response.data) {
      return null;
    }

    let data;
    try {
      data = JSON.parse(response.data);
    } catch {
      return null;
    }

    const errors = Array.isArray(data) ? data.map(item => item && item.error) : [data.error];
    for (const error of errors) {
      const status = String(error?.status || '').toUpperCase();
      const message = String(error?.message || '').toLowerCase();

      if (message.includes('valid api key')
        || message.includes('api key not valid')
        || message.includes('invalid api key')
        || message.includes('permission denied for api key')
        || (status === 'INVALID_ARGUMENT' && message.includes('api key'))
        || status === 'PERMISSION_DENIED') {
        return error.message || status;
      }
    }

    return null;
  }

  isOpenAICompatibleEndpoint(pathname) {
    return /\/openai\//.test(pathname) || /\/chat\/completions$/.test(pathname);
  }

  normalizeOpenAICompatibleUrl(url) {
    if (/\/openai\//.test(url.pathname) || !/\/chat\/completions$/.test(url.pathname)) {
      return;
    }

    url.pathname = url.pathname.replace(/\/chat\/completions$/, '/openai/chat/completions');
  }

  maskApiKey(key) {
    if (!key || key.length < 8) return '***';
    return key.substring(0, 4) + '...' + key.substring(key.length - 4);
  }
}

module.exports = GeminiClient;
