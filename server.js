/**
 * Buhdi WS — WebSocket relay for buhdi-node connections
 * 
 * Runs on Fly.io. Maintains persistent WebSocket connections with nodes.
 * Vercel (mybuhdi.com) dispatches tasks via POST /dispatch.
 * Nodes receive tasks in real-time instead of polling.
 */

const http = require('http');
const { WebSocketServer, WebSocket } = require('ws');
const { createClient } = require('@supabase/supabase-js');

// ── Config ──────────────────────────────────────────────
const PORT = process.env.PORT || 8080;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const DISPATCH_SECRET = process.env.DISPATCH_SECRET; // shared secret with Vercel
const HEARTBEAT_INTERVAL = 30_000;  // 30s
const HEARTBEAT_TIMEOUT = 90_000;   // 90s — mark offline if no pong
const STALE_CHECK_INTERVAL = 15_000; // check for stale connections every 15s

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !DISPATCH_SECRET) {
  console.error('Missing required env vars: SUPABASE_URL, SUPABASE_SERVICE_KEY, DISPATCH_SECRET');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// ── Connection Registry ─────────────────────────────────
// Map<nodeId, { ws, userId, nodeName, lastPong }>
const connections = new Map();

// Reverse lookup: Map<bnode_key, nodeId> for fast auth
const keyToNode = new Map();

function log(msg, ...args) {
  console.log(`[${new Date().toISOString()}] ${msg}`, ...args);
}

// ── Authentication ──────────────────────────────────────
async function authenticateNode(apiKey) {
  if (!apiKey || !apiKey.startsWith('bnode_')) return null;

  const { data, error } = await supabase
    .from('node_registry')
    .select('id, user_id, node_name')
    .eq('api_key', apiKey)
    .single();

  if (error || !data) return null;
  return data;
}

// ── Update node status in DB ────────────────────────────
async function setNodeStatus(nodeId, status) {
  await supabase
    .from('node_registry')
    .update({ status, last_heartbeat: new Date().toISOString() })
    .eq('id', nodeId);
}

// ── Dispatch pending tasks to a node ────────────────────
async function flushPendingTasks(nodeId) {
  const { data: tasks } = await supabase
    .from('node_tasks')
    .select('id, type, payload')
    .eq('node_id', nodeId)
    .eq('status', 'pending')
    .order('created_at', { ascending: true })
    .limit(20);

  if (!tasks || tasks.length === 0) return;

  const conn = connections.get(nodeId);
  if (!conn || conn.ws.readyState !== WebSocket.OPEN) return;

  for (const task of tasks) {
    conn.ws.send(JSON.stringify({
      type: 'task',
      task: { id: task.id, type: task.type, payload: task.payload }
    }));

    // Mark as dispatched
    await supabase
      .from('node_tasks')
      .update({ status: 'dispatched', dispatched_at: new Date().toISOString() })
      .eq('id', task.id);

    log(`Pushed task ${task.id} (${task.type}) to node ${nodeId}`);
  }
}

// ── Handle incoming WebSocket messages from node ────────
async function handleNodeMessage(nodeId, data) {
  let msg;
  try {
    msg = JSON.parse(data);
  } catch {
    return;
  }

  const conn = connections.get(nodeId);

  switch (msg.type) {
    case 'pong':
      if (conn) conn.lastPong = Date.now();
      break;

    case 'heartbeat':
      // Update DB heartbeat
      if (conn) conn.lastPong = Date.now();
      await setNodeStatus(nodeId, 'online');
      break;

    case 'result':
      // Task result from node
      if (msg.taskId && msg.result) {
        await supabase
          .from('node_tasks')
          .update({
            status: msg.result.status === 'failed' ? 'failed' : 'completed',
            result: msg.result.result || null,
            error: msg.result.error || null,
            completed_at: new Date().toISOString()
          })
          .eq('id', msg.taskId);
        log(`Result for task ${msg.taskId}: ${msg.result.status}`);
      }
      break;

    default:
      log(`Unknown message type from node ${nodeId}: ${msg.type}`);
  }
}

// ── HTTP Server ─────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  // Health check
  if (req.method === 'GET' && req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      connections: connections.size,
      uptime: process.uptime()
    }));
    return;
  }

  // Dispatch endpoint — Vercel pushes tasks here
  if (req.method === 'POST' && req.url === '/dispatch') {
    // Verify shared secret
    const auth = req.headers['x-dispatch-secret'];
    if (auth !== DISPATCH_SECRET) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }

    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      try {
        const { nodeId, taskId, type, payload } = JSON.parse(body);

        const conn = connections.get(nodeId);
        if (conn && conn.ws.readyState === WebSocket.OPEN) {
          conn.ws.send(JSON.stringify({
            type: 'task',
            task: { id: taskId, type, payload }
          }));

          // Mark as dispatched
          await supabase
            .from('node_tasks')
            .update({ status: 'dispatched', dispatched_at: new Date().toISOString() })
            .eq('id', taskId);

          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ status: 'pushed' }));
          log(`Dispatch: pushed task ${taskId} to node ${nodeId}`);
        } else {
          // Node not connected — task stays pending for polling fallback
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ status: 'queued', reason: 'node_not_connected' }));
          log(`Dispatch: node ${nodeId} not connected, task ${taskId} stays queued`);
        }
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Invalid request' }));
      }
    });
    return;
  }

  // Status page
  if (req.method === 'GET' && req.url === '/status') {
    const nodes = [];
    for (const [nodeId, conn] of connections) {
      nodes.push({
        nodeId,
        nodeName: conn.nodeName,
        connected: conn.ws.readyState === WebSocket.OPEN,
        lastPong: new Date(conn.lastPong).toISOString()
      });
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ connections: nodes.length, nodes }));
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

// ── WebSocket Server ────────────────────────────────────
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', async (ws, req) => {
  // Extract API key from query string
  const url = new URL(req.url, `http://localhost`);
  const apiKey = url.searchParams.get('key');

  // Authenticate
  const node = await authenticateNode(apiKey);
  if (!node) {
    ws.close(4001, 'Authentication failed');
    log('Connection rejected: invalid key');
    return;
  }

  // Close existing connection for this node (if any)
  const existing = connections.get(node.id);
  if (existing && existing.ws.readyState === WebSocket.OPEN) {
    existing.ws.close(4002, 'Replaced by new connection');
  }

  // Register connection
  connections.set(node.id, {
    ws,
    userId: node.user_id,
    nodeName: node.node_name,
    lastPong: Date.now()
  });

  await setNodeStatus(node.id, 'online');
  log(`Node connected: "${node.node_name}" (${node.id})`);

  // Flush any pending tasks
  await flushPendingTasks(node.id);

  // Handle messages
  ws.on('message', (data) => {
    handleNodeMessage(node.id, data.toString());
  });

  // Handle disconnect
  ws.on('close', async (code, reason) => {
    connections.delete(node.id);
    await setNodeStatus(node.id, 'offline');
    log(`Node disconnected: "${node.node_name}" (${node.id}) code=${code}`);
  });

  ws.on('error', (err) => {
    log(`WebSocket error for "${node.node_name}": ${err.message}`);
  });

  // Send welcome
  ws.send(JSON.stringify({
    type: 'welcome',
    nodeId: node.id,
    nodeName: node.node_name
  }));
});

// ── Heartbeat / Stale Connection Cleanup ────────────────
setInterval(() => {
  const now = Date.now();
  for (const [nodeId, conn] of connections) {
    if (conn.ws.readyState !== WebSocket.OPEN) {
      connections.delete(nodeId);
      continue;
    }

    // Send ping
    conn.ws.send(JSON.stringify({ type: 'ping' }));

    // Check for stale (no pong in HEARTBEAT_TIMEOUT)
    if (now - conn.lastPong > HEARTBEAT_TIMEOUT) {
      log(`Node "${conn.nodeName}" timed out — closing`);
      conn.ws.close(4003, 'Heartbeat timeout');
      connections.delete(nodeId);
      setNodeStatus(nodeId, 'offline');
    }
  }
}, STALE_CHECK_INTERVAL);

// ── Start ───────────────────────────────────────────────
server.listen(PORT, '0.0.0.0', () => {
  log(`Buhdi WS relay listening on port ${PORT}`);
  log(`WebSocket: ws://0.0.0.0:${PORT}/ws`);
  log(`Dispatch: POST http://0.0.0.0:${PORT}/dispatch`);
  log(`Health: GET http://0.0.0.0:${PORT}/health`);
});

// Graceful shutdown
for (const sig of ['SIGTERM', 'SIGINT']) {
  process.on(sig, () => {
    log(`${sig} received — closing connections`);
    for (const [, conn] of connections) {
      conn.ws.close(1001, 'Server shutting down');
    }
    server.close(() => process.exit(0));
  });
}
