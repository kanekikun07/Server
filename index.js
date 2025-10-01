const ProxyChain = require('proxy-chain');
const socks = require('socksv5');
const fs = require('fs');
const url = require('url');
const net = require('net');
const path = require('path');
const logger = require('./logger');
const express = require('express');
const path = require('path');
const app = express();


// Serve user_proxies.txt publicly


// Load proxies from file (no auth), filter out https and socks4 proxies
const proxyList = fs.readFileSync('proxies.txt', 'utf-8')
  .split('\n')
  .map(line => line.trim())
  .filter(line => line.length > 0)
  .filter(line => {
    const lower = line.toLowerCase();
    return !lower.startsWith('https://') && !lower.startsWith('socks4://');
  });

if (proxyList.length === 0) {
  logger.error('No valid HTTP proxies found in proxies.txt after filtering out https and socks4 proxies.');
  process.exit(1);
}

// Dynamically generate USERS object based on proxies
const USERS = {};
proxyList.forEach((proxy, index) => {
  const username = `user${index + 1}`;
  const password = `pass${index + 1}`;
  USERS[username] = { password, proxyIndex: index };
});

logger.info('Generated USERS: ' + JSON.stringify(USERS));

// Usage tracking object
const usageStats = {}; // { username: { bytesSent, bytesReceived, requests } }

// Helper to log usage
function logUsage(username, bytesSent, bytesReceived) {
  if (!usageStats[username]) {
    usageStats[username] = { bytesSent: 0, bytesReceived: 0, requests: 0 };
  }
  usageStats[username].bytesSent += bytesSent;
  usageStats[username].bytesReceived += bytesReceived;
  usageStats[username].requests += 1;
}

// --- HTTP/HTTPS Proxy Server with proxy-chain ---

const httpProxyServer = new ProxyChain.Server({
  port: 8000,

  prepareRequestFunction: ({ username, password, request }) => {
    if (!username || !password) {
      logger.info(`[HTTP Proxy] Authentication required for request to ${request.url}`);
      return {
        responseCode: 407,
        responseHeaders: {
          'Proxy-Authenticate': 'Basic realm="Proxy Relay"',
        },
        body: 'Proxy authentication required',
      };
    }

    const user = USERS[username];
    if (!user || user.password !== password) {
      logger.info(`[HTTP Proxy] Invalid credentials for user: ${username}`);
      return {
        responseCode: 403,
        body: 'Invalid username or password',
      };
    }

    const upstreamProxyUrl = proxyList[user.proxyIndex];
    if (!upstreamProxyUrl) {
      logger.error(`[HTTP Proxy] No upstream proxy assigned for user: ${username}`);
      return {
        responseCode: 500,
        body: 'No upstream proxy assigned',
      };
    }

    logger.info(`[HTTP Proxy] User: ${username} requested ${request.url}`);

    return {
      upstreamProxyUrl,
      userInfo: { username, requestUrl: request.url },
    };
  },

  handleRequestFinished: ({ userInfo, bytesRead, bytesWritten }) => {
    if (userInfo && userInfo.username) {
      logUsage(userInfo.username, bytesWritten, bytesRead);
      logger.info(`[HTTP Proxy] User: ${userInfo.username} - Sent: ${bytesWritten} bytes, Received: ${bytesRead} bytes`);

      // Traffic log (structured)
      logger.info({
        type: 'http_traffic',
        user: userInfo.username,
        url: userInfo.requestUrl,
        bytesSent: bytesWritten,
        bytesReceived: bytesRead,
        timestamp: new Date().toISOString()
      }, 'Traffic log');
    }
  },
});

httpProxyServer.listen(() => {
  logger.info(`HTTP/HTTPS Proxy Relay running on port ${httpProxyServer.port}`);
});

// --- SOCKS4/5 Proxy Server with socksv5 ---

const socksServer = socks.createServer((info, accept, deny) => {
  // Deny here because we handle connection in 'proxyConnect' event
  deny();
});

socksServer.useAuth(socks.auth.UserPassword((user, password, cb) => {
  const userData = USERS[user];
  if (userData && userData.password === password) {
    cb(true);
  } else {
    cb(false);
  }
}));

socksServer.on('proxyConnect', (info, destination, socket, head) => {
  const user = info.userId;
  if (!user || !USERS[user]) {
    logger.info(`[SOCKS Proxy] Connection denied: unknown user '${user}'`);
    socket.end();
    return;
  }

  const userData = USERS[user];
  const upstreamProxy = proxyList[userData.proxyIndex];
  if (!upstreamProxy) {
    logger.error(`[SOCKS Proxy] No upstream proxy assigned for user: ${user}`);
    socket.end();
    return;
  }

  logger.info(`[SOCKS Proxy] User: ${user} connecting to ${info.dstAddr}:${info.dstPort} via upstream proxy ${upstreamProxy}`);

  const parsedProxy = url.parse(upstreamProxy);

  const proxySocket = net.connect(parsedProxy.port, parsedProxy.hostname, () => {
    const connectReq = `CONNECT ${info.dstAddr}:${info.dstPort} HTTP/1.1\r\nHost: ${info.dstAddr}:${info.dstPort}\r\n\r\n`;
    proxySocket.write(connectReq);
  });

  proxySocket.once('data', (chunk) => {
    const response = chunk.toString();
    if (/^HTTP\/1\.[01] 200/.test(response)) {
      socket.write(chunk);
      proxySocket.pipe(socket);
      socket.pipe(proxySocket);

      let bytesSent = 0;
      let bytesReceived = 0;

      socket.on('data', (data) => {
        bytesSent += data.length;
        logger.debug(`[SOCKS Proxy] User: ${user} sent ${data.length} bytes`);
      });
      proxySocket.on('data', (data) => {
        bytesReceived += data.length;
        logger.debug(`[SOCKS Proxy] User: ${user} received ${data.length} bytes`);
      });

      socket.on('close', () => {
        logUsage(user, bytesSent, bytesReceived);
        logger.info(`[SOCKS Proxy] User: ${user} connection closed. Total sent: ${bytesSent} bytes, received: ${bytesReceived} bytes`);

        // Traffic log (structured)
        logger.info({
          type: 'socks_traffic',
          user,
          destination: `${info.dstAddr}:${info.dstPort}`,
          bytesSent,
          bytesReceived,
          timestamp: new Date().toISOString()
        }, 'Traffic log');
      });

      socket.on('error', (err) => {
        logger.error(`[SOCKS Proxy] User: ${user} client socket error: ${err.message || err}`);
      });

      proxySocket.on('error', (err) => {
        logger.error(`[SOCKS Proxy] User: ${user} upstream proxy socket error: ${err.message || err}`);
      });
    } else {
      logger.info(`[SOCKS Proxy] User: ${user} upstream proxy connection failed with response: ${response.split('\r\n')[0]}`);
      socket.end();
      proxySocket.end();
    }
  });

  proxySocket.on('error', (err) => {
    logger.error(`[SOCKS Proxy] User: ${user} upstream proxy socket error: ${err.message || err}`);
    socket.end();
  });

  socket.on('error', (err) => {
    logger.error(`[SOCKS Proxy] User: ${user} client socket error: ${err.message || err}`);
    proxySocket.end();
  });
});

socksServer.listen(1080, '0.0.0.0', () => {
  logger.info('SOCKS4/5 Proxy Relay running on port 1080');
  writeUserProxiesFile();
});

socksServer.on('error', (err) => {
  logger.error('SOCKS server error: ' + (err.message || err));
});

// --- Write user proxies file ---

function writeUserProxiesFile() {
  const lines = [];

  Object.entries(USERS).forEach(([username, { password, proxyIndex }]) => {
    const upstreamProxy = proxyList[proxyIndex];
    if (!upstreamProxy) return;

    // Parse upstream proxy URL to extract protocol and host:port
    const parsed = url.parse(upstreamProxy);

    // Compose line: protocol://username:password@host:port
    const line = `${parsed.protocol}//${encodeURIComponent(username)}:${encodeURIComponent(password)}@${parsed.host}`;
    lines.push(line);
  });

  const filePath = path.resolve(__dirname, 'user_proxies.txt');
  fs.writeFileSync(filePath, lines.join('\n'), 'utf-8');
  logger.info(`User  proxy list saved to ${filePath}`);
}

// --- Handle uncaught exceptions and rejections ---

app.get('/user_proxies.txt', (req, res) => {
  const filePath = path.resolve(__dirname, 'user_proxies.txt');
  res.sendFile(filePath);
});
app.listen(3000, () => {
  console.log('Public file server running at http://localhost:3000/user_proxies.txt');
});

process.on('uncaughtException', (err) => {
  logger.error(`Uncaught Exception: ${err.stack || err}`);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error(`Unhandled Rejection at: ${promise}, reason: ${reason}`);

});
