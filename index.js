// Proxy Relay Server - Scaled for 3000+ Proxies
// Features:
// - Dynamic proxy reloading via file watcher (chokidar).
// - Proxy health checks with failover support (axios) - Runs after proxy loading and periodically.
// - Separate /health endpoint to view/report health status (JSON response with healthy/total counts).
// - Centralized logging with rotation (rotating-file-stream) - All logs public via /logs.
// - Connection pooling for upstream proxies (basic Map-based with limits).
// - Usage tracking and metrics (prom-client) exposed at /metrics.
// - Public endpoints for user_proxies.txt and logs (no auth, no pagination).
// - Async file I/O for better performance.
// - Error recovery and structured logging.
// - Filters out invalid proxies (https://, socks4://).
// Dependencies: npm i proxy-chain socksv5 express chokidar axios rotating-file-stream prom-client mkdirp

const ProxyChain = require('proxy-chain');
const socks = require('socksv5');
const fs = require('fs').promises;
const url = require('url');
const net = require('net');
const path = require('path');
const express = require('express');
const chokidar = require('chokidar');
const axios = require('axios');
const rfs = require('rotating-file-stream');
const client = require('prom-client');
const mkdirp = require('mkdirp');

// Centralized logging with rotation - All logs captured and public
const logDir = path.resolve(__dirname, 'logs');
mkdirp.sync(logDir);
const logStream = rfs.createStream('proxy_logs.txt', {
  interval: '1d', // Rotate daily
  size: '10M',    // Or at 10MB
  compress: 'gzip',
  path: logDir
});

function customLog(level, message) {
  const timestamp = new Date().toISOString();
  const logEntry = `[${timestamp}] [${level.toUpperCase()}] ${typeof message === 'object' ? JSON.stringify(message) : message}\n`;
  logStream.write(logEntry);
  console.log(logEntry.trim()); // Also to console
}

// Global variables
let proxyList = [];
let USERS = {};
let activeProxies = new Set(); // Healthy proxies
const usageStats = {}; // { username: { bytesSent, bytesReceived, requests } }

// Metrics
const healthyProxiesGauge = new client.Gauge({ name: 'healthy_proxies_total', help: 'Total healthy proxies' });
const totalRequestsCounter = new client.Counter({ name: 'total_requests', help: 'Total proxy requests' });

// Helper to log usage
function logUsage(username, bytesSent, bytesReceived) {
  if (!usageStats[username]) {
    usageStats[username] = { bytesSent: 0, bytesReceived: 0, requests: 0 };
  }
  usageStats[username].bytesSent += bytesSent;
  usageStats[username].bytesReceived += bytesReceived;
  usageStats[username].requests += 1;
  totalRequestsCounter.inc();
}

// Load proxies (async, with filtering)
async function loadProxies() {
  try {
    const data = await fs.readFile('proxies.txt', 'utf-8');
    proxyList = data.split('\n')
      .map(line => line.trim())
      .filter(line => line.length > 0)
      .filter(line => {
        const lower = line.toLowerCase();
        return !lower.startsWith('https://') && !lower.startsWith('socks4://');
      });

    if (proxyList.length === 0) {
      customLog('error', 'No valid HTTP proxies found in proxies.txt after filtering.');
      process.exit(1);
    }

    // Dynamically generate USERS based on proxies
    USERS = {};
    proxyList.forEach((proxy, index) => {
      const username = `kevin${index + 1}`;
      const password = `pass${index + 1}`;
      USERS[username] = { password, proxyIndex: index };
    });

    activeProxies = new Set(proxyList); // Assume all active initially
    healthyProxiesGauge.set(proxyList.length);

    customLog('info', `Loaded ${proxyList.length} proxies and generated ${Object.keys(USERS).length} users`);

    await writeUserProxiesFile();
  } catch (err) {
    customLog('error', `Failed to load proxies: ${err.message}`);
    process.exit(1);
  }
}

// Health check function (runs periodically) - Separate function for modularity
async function healthCheck() {
  if (proxyList.length === 0) {
    customLog('warn', 'No proxies loaded, skipping health check');
    return;
  }

  customLog('info', 'Starting health check...');
  const healthy = new Set();
  // Parallelize for speed with 3000+ proxies (using Promise.allSettled)
  const checks = proxyList.map(async (proxyUrl) => {
    try {
      const parsed = url.parse(proxyUrl);
      await axios.get('http://httpbin.org/ip', {
        proxy: { host: parsed.hostname, port: parsed.port || 8080 },
        timeout: 5000
      });
      return proxyUrl;
    } catch (err) {
      customLog('warn', `Health check failed for ${proxyUrl}: ${err.message}`);
      return null;
    }
  });

  const results = await Promise.allSettled(checks);
  results.forEach((result, index) => {
    if (result.status === 'fulfilled' && result.value) {
      healthy.add(result.value);
    }
  });

  activeProxies = healthy;
  healthyProxiesGauge.set(healthy.size);
  customLog('info', `Health check completed: ${healthy.size}/${proxyList.length} healthy proxies`);
}

// Write user proxies file (async)
async function writeUserProxiesFile() {
  const lines = [];
  Object.entries(USERS).forEach(([username, { password, proxyIndex }]) => {
    const upstreamProxy = proxyList[proxyIndex];
    if (!upstreamProxy || !activeProxies.has(upstreamProxy)) return; // Skip unhealthy
    const parsed = url.parse(upstreamProxy);
    const line = `${parsed.protocol}//${encodeURIComponent(username)}:${encodeURIComponent(password)}@${parsed.host}`;
    lines.push(line);
  });
  const filePath = path.resolve(__dirname, 'user_proxies.txt');
  await fs.writeFile(filePath, lines.join('\n'), 'utf-8');
  customLog('info', `User  proxy list saved to ${filePath} (${lines.length} entries)`);
}

// Basic upstream proxy connection pool (Map-based, with refCount limits)
const proxyPool = new Map(); // key: `${hostname}:${port}` -> { socket: net.Socket, refCount: number }

// Get or create pooled connection
function getPooledConnection(proxyUrl, callback) {
  const parsed = url.parse(proxyUrl);
  const key = `${parsed.hostname}:${parsed.port || 8080}`;
  const maxConnections = 50; // Per-proxy limit for scale

  if (proxyPool.has(key)) {
    const entry = proxyPool.get(key);
    if (entry.refCount < maxConnections && entry.socket && !entry.socket.destroyed) {
      entry.refCount += 1;
      proxyPool.set(key, entry);
      return callback(null, entry.socket);
    } else if (entry.refCount >= maxConnections) {
      return callback(new Error(`Max connections reached for ${key}`));
    } else {
      proxyPool.delete(key);
    }
  }

  const newSocket = net.connect(parsed.port || 8080, parsed.hostname, () => {
    const entry = { socket: newSocket, refCount: 1 };
    proxyPool.set(key, entry);
    callback(null, newSocket);
  });

  newSocket.on('error', (err) => {
    if (proxyPool.has(key)) proxyPool.delete(key);
    customLog('error', `Pooled connection error for ${key}: ${err.message}`);
    callback(err);
  });

  newSocket.on('close', () => {
    if (proxyPool.has(key)) {
      const entry = proxyPool.get(key);
      entry.refCount -= 1;
      if (entry.refCount <= 0) {
        proxyPool.delete(key);
        customLog('debug', `Pooled connection fully closed for ${key}`);
      } else {
        proxyPool.set(key, entry);
      }
    }
  });
}

// Release connection
function releaseConnection(proxyUrl) {
  const parsed = url.parse(proxyUrl);
  const key = `${parsed.hostname}:${parsed.port || 8080}`;
  if (proxyPool.has(key)) {
    const entry = proxyPool.get(key);
    entry.refCount -= 1;
    if (entry.refCount <= 0) {
      proxyPool.delete(key);
    } else {
      proxyPool.set(key, entry);
    }
    customLog('debug', `Released connection for ${key} (refCount: ${entry.refCount})`);
  }
}

// --- HTTP/HTTPS Proxy Server with proxy-chain ---
const httpProxyServer = new ProxyChain.Server({
  port: 8000,

  prepareRequestFunction: ({ username, password, request }) => {
    if (!username || !password) {
      customLog('info', `[HTTP Proxy] Authentication required for request to ${request.url}`);
      return {
        responseCode: 407,
        responseHeaders: { 'Proxy-Authenticate': 'Basic realm="Proxy Relay"' },
        body: 'Proxy authentication required',
      };
    }

    const user = USERS[username];
    if (!user || user.password !== password) {
      customLog('info', `[HTTP Proxy] Invalid credentials for user: ${username}`);
      return {
        responseCode: 403,
        body: 'Invalid username or password',
      };
    }

    let upstreamProxyUrl = proxyList[user.proxyIndex];
    if (!upstreamProxyUrl || !activeProxies.has(upstreamProxyUrl)) {
      // Failover: Find next healthy proxy
      const healthyList = Array.from(activeProxies);
      let healthyIndex = healthyList.findIndex(p => p === upstreamProxyUrl);
      if (healthyIndex === -1) healthyIndex = -1; // If current not healthy, start from 0
      const nextIndex = (healthyIndex + 1) % healthyList.length;
      upstreamProxyUrl = healthyList[nextIndex] || healthyList[0];
      if (!upstreamProxyUrl) {
        customLog('error', `[HTTP Proxy] No healthy upstream proxy for user: ${username}`);
        return { responseCode: 503, body: 'No healthy proxies available' };
      }
    }

    customLog('info', `[HTTP Proxy] User: ${username} requested ${request.url} via ${upstreamProxyUrl}`);

    return {
      upstreamProxyUrl,
      userInfo: { username, requestUrl: request.url },
    };
  },

  handleRequestFinished: ({ userInfo, bytesRead, bytesWritten }) => {
    if (userInfo && userInfo.username) {
      logUsage(userInfo.username, bytesWritten, bytesRead);
      customLog('info', `[HTTP Proxy] User: ${userInfo.username} - Sent: ${bytesWritten} bytes, Received: ${bytesRead} bytes`);

      customLog('info', {
        type: 'http_traffic',
        user: userInfo.username,
        url: userInfo.requestUrl,
        bytesSent: bytesWritten,
        bytesReceived: bytesRead,
        timestamp: new Date().toISOString()
      });
    }
  },
});

httpProxyServer.listen(() => {
  customLog('info', `HTTP/HTTPS Proxy Relay running on port 8000`);
});

httpProxyServer.on('error', (err) => {
  customLog('error', `HTTP Proxy Server error: ${err.message || err}`);
});

// --- SOCKS4/5 Proxy Server with socksv5 (with pooling and failover) ---
const socksServer = socks.createServer((info, accept, deny) => {
  deny(); // Handle in 'proxyConnect'
});

socksServer.useAuth(socks.auth.UserPassword((user, password, cb) => {
  const userData = USERS[user];
  if (userData && userData.password === password) {
    cb(true);
  } else {
    customLog('info', `[SOCKS Auth] Invalid credentials for user: ${user}`);
    cb(false);
  }
}));

socksServer.on('proxyConnect', (info, destination, socket, head) => {
  const user = info.userId;
  if (!user || !USERS[user]) {
    customLog('info', `[SOCKS Proxy] Connection denied: unknown user '${user}'`);
    socket.end();
    return;
  }

  const userData = USERS[user];
  let upstreamProxy = proxyList[userData.proxyIndex];
  if (!upstreamProxy || !activeProxies.has(upstreamProxy)) {
    // Failover: Find next healthy
    const healthyList = Array.from(activeProxies);
    let healthyIndex = healthyList.findIndex(p => p === upstreamProxy);
    if (healthyIndex === -1) healthyIndex = -1;
    const nextIndex = (healthyIndex + 1) % healthyList.length;
    upstreamProxy = healthyList[nextIndex] || healthyList[0];
    if (!upstreamProxy) {
      customLog('error', `[SOCKS Proxy] No healthy upstream proxy for user: ${user}`);
      socket.end();
      return;
    }
  }

  customLog('info', `[SOCKS Proxy] User: ${user} connecting to ${info.dstAddr}:${info.dstPort} via ${upstreamProxy}`);

  getPooledConnection(upstreamProxy, (err, proxySocket) => {
    if (err) {
      customLog('error', `[SOCKS Proxy] Failed to get pooled connection to ${upstreamProxy}: ${err.message}`);
      socket.end();
      return;
    }

    const connectReq = `CONNECT ${info.dstAddr}:${info.dstPort} HTTP/1.1\r\nHost: ${info.dstAddr}:${info.dstPort}\r\n\r\n`;
    proxySocket.write(connectReq);

    proxySocket.once('data', (chunk) => {
      const response = chunk.toString();
      if (/^HTTP\/1\.[01] 200/.test(response)) {
        if (head && head.length) socket.write(head);
        socket.write(chunk);

        let bytesSent = (head ? head.length : 0) + chunk.length;
        let bytesReceived = 0;

        proxySocket.pipe(socket);
        socket.pipe(proxySocket);

        socket.on('data', (data) => {
          bytesSent += data.length;
          customLog('debug', `[SOCKS Proxy] User: ${user} sent ${data.length} bytes`);
        });

        proxySocket.on('data', (data) => {
          bytesReceived += data.length;
          customLog('debug', `[SOCKS Proxy] User: ${user} received ${data.length} bytes`);
        });

        const onClose = () => {
          releaseConnection(upstreamProxy);
          logUsage(user, bytesSent, bytesReceived);
          customLog('info', `[SOCKS Proxy] User: ${user} connection closed. Total sent: ${bytesSent} bytes, received: ${bytesReceived} bytes`);

          customLog('info', {
            type: 'socks_traffic',
            user,
            destination: `${info.dstAddr}:${info.dstPort}`,
            bytesSent,
            bytesReceived,
            timestamp: new Date().toISOString()
          });
        };

        socket.on('close', onClose);
        proxySocket.on('close', onClose);

        socket.on('error', (err) => {
          customLog('error', `[SOCKS Proxy] User: ${user} client error: ${err.message || err}`);
          releaseConnection(upstreamProxy);
          proxySocket.end();
        });

        proxySocket.on('error', (err) => {
          customLog('error', `[SOCKS Proxy] User: ${user} upstream error: ${err.message || err}`);
          releaseConnection(upstreamProxy);
          socket.end();
        });
      } else {
        customLog('info', `[SOCKS Proxy] User: ${user} upstream failed: ${response.split('\r\n')[0]}`);
        releaseConnection(upstreamProxy);
        socket.end();
        proxySocket.end();
      }
    });

    proxySocket.on('error', (err) => {
      customLog('error', `[SOCKS Proxy] User: ${user} upstream socket error: ${err.message || err}`);
      releaseConnection(upstreamProxy);
      socket.end();
    });

    socket.on('error', (err) => {
      customLog('error', `[SOCKS Proxy] User: ${user} client socket error: ${err.message || err}`);
      releaseConnection(upstreamProxy);
      proxySocket.end();
    });
  });
});

socksServer.listen(1080, '0.0.0.0', () => {
  customLog('info', 'SOCKS4/5 Proxy Relay running on port 1080');
});

socksServer.on('error', (err) => {
  customLog('error', `SOCKS server error: ${err.message || err}`);
});

// --- Express server for public endpoints (no auth, no pagination) ---
const app = express();

app.get('/user_proxies.txt', async (req, res) => {
  const filePath = path.resolve(__dirname, 'user_proxies.txt');
  try {
    const data = await fs.readFile(filePath, 'utf-8');
    res.type('text/plain').send(data || 'No proxies available.');
    customLog('info', `Served full user_proxies.txt to ${req.ip}`);
  } catch (err) {
    customLog('warn', `Access to /user_proxies.txt failed: ${err.message}`);
    res.status(404).send('user_proxies.txt not found');
  }
});

// Public logs at / (tailed for large files)
app.get('/', async (req, res) => {
  const logPath = path.join(logDir, 'proxy_logs.txt'); // Latest log file
  try {
    const data = await fs.readFile(logPath, 'utf-8');
    // Tail last 10k chars for performance with large logs
    const tailed = data.length > 10000 ? data.slice(-10000) : data;
    res.type('text/plain').send(tailed || 'No logs yet.');
    customLog('info', `Served public logs (tailed) to ${req.ip}`);
  } catch (err) {
    res.status(404).send('Logs not found');
    customLog('warn', `Failed to serve logs: ${err.message}`);
  }
});

// Full logs endpoint (full file, for small logs)
app.get('/logs', async (req, res) => {
  const logPath = path.join(logDir, 'proxy_logs.txt');
  try {
    const data = await fs.readFile(logPath, 'utf-8');
    res.type('text/plain').send(data || 'No logs yet.');
    customLog('info', `Served full logs to ${req.ip}`);
  } catch (err) {
    res.status(404).send('Logs not found');
    customLog('warn', `Failed to serve full logs: ${err.message}`);
  }
});

// Separate health check endpoint (JSON response, public for monitoring)
app.get('/health', (req,  res) => {
  const { details = 'false' } = req.query; // Optional: ?details=true to list healthy proxies
  const healthStatus = {
    totalProxies: proxyList.length,
    healthyProxies: activeProxies.size,
    healthyPercentage: proxyList.length > 0 ? Math.round((activeProxies.size / proxyList.length) * 100) : 0,
    lastChecked: new Date().toISOString(), // Approximate; could track exact timestamp
    status: activeProxies.size > 0 ? 'healthy' : 'degraded'
  };

  if (details === 'true') {
    healthStatus.healthyList = Array.from(activeProxies);
    healthStatus.unhealthyList = proxyList.filter(p => !activeProxies.has(p));
  }

  res.json(healthStatus);
  customLog('info', `Health status requested by ${req.ip} (details: ${details})`);
});

// Metrics endpoint (public, no auth for monitoring tools)
app.get('/metrics', async (req, res) => {
  res.set('Content-Type', client.register.contentType);
  res.end(await client.register.metrics());
});

app.listen(3000, '0.0.0.0', () => {
  customLog('info', 'Public file server running at http://localhost:3000/');
  customLog('info', 'Public logs available at http://localhost:3000/ (tailed)');
  customLog('info', 'Full logs at http://localhost:3000/logs');
  customLog('info', 'Full user_proxies.txt at http://localhost:3000/user_proxies.txt');
  customLog('info', 'Health status at http://localhost:3000/health (JSON, public)');
  customLog('info', 'Metrics at http://localhost:3000/metrics (public)');
});

// Dynamic proxy reloading (watches proxies.txt for changes)
chokidar.watch('proxies.txt').on('change', async () => {
  customLog('info', 'proxies.txt changed, reloading...');
  await loadProxies();
  await healthCheck(); // Re-check health after reload
});

// Initialization - Load proxies first, then run initial health check
async function init() {
  await loadProxies(); // Load all proxies
  customLog('info', 'Proxies loaded, starting initial health check...');
  await healthCheck(); // Run health check after loading
  setInterval(healthCheck, 5 * 60 * 1000); // Run periodically every 5 minutes
}

init().catch(err => {
  customLog('error', `Initialization failed: ${err.message}`);
  process.exit(1);
});

// --- Handle uncaught exceptions and rejections (all logged publicly) ---
process.on('uncaughtException', (err) => {
  customLog('error', `Uncaught Exception: ${err.stack || err}`);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  customLog('error', `Unhandled Rejection at: ${promise}, reason: ${reason}`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  customLog('info', 'SIGINT received, shutting down gracefully');
  httpProxyServer.close(() => {});
  socksServer.close(() => {});
  process.exit(0);
});

process.on('SIGTERM', () => {
  customLog('info', 'SIGTERM received, shutting down gracefully');
  httpProxyServer.close(() => {});
  socksServer.close(() => {});
  process.exit(0);
});
