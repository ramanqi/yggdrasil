const WebSocket = require("ws");
const express = require("express");
const http = require("http");

const app = express();
app.use(express.json({ limit: "10mb" })); // For large track payloads
const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

// State Stores
const edgeNodes = new Map(); // Map<NodeId, { wsLavalink, wsRest, stats }>
const botSessions = new Map(); // Map<BotUserId, EdgeNodeId>
const passphrase = process.env?.PASSPHRASE || "super_secret_hub_password";

// HANDLE EDGE NODE CONNECTIONS (Outbound from Lavalink)
server.on("upgrade", (request, socket, head) => {
    const nodeId = request.headers["x-node-id"];
    const channel = request.headers["x-channel"] || "lavalink"; // "lavalink" or "rest"
    const auth = request.headers["authorization"];

    if (!nodeId || auth !== passphrase) {
        socket.destroy();
        return;
    }

    wss.handleUpgrade(request, socket, head, (ws) => {
        ws.nodeId = nodeId;
        ws.channel = channel;
        wss.emit("connection", ws, request);
    });
});

wss.on("connection", (ws) => {
    const { nodeId, channel } = ws;
    console.log(`[Hub] Edge Node ${nodeId} connected on channel: ${channel}`);

    if (!edgeNodes.has(nodeId)) {
        edgeNodes.set(nodeId, { wsLavalink: null, wsRest: null, stats: { players: 0 } });
    }
    const node = edgeNodes.get(nodeId);

    if (channel === "lavalink") node.wsLavalink = ws;
    if (channel === "rest") node.wsRest = ws;

    ws.on("message", (data) => {
        try {
            const msg = JSON.parse(data);
            if (msg.op === "stats") node.stats = msg;
            
            // Handle REST RPC responses from Edge Node
            if (channel === "rest" && msg.rpcId) {
                // We"ll handle this in the REST proxy section below
                if (global.restRpcCallbacks && global.restRpcCallbacks[msg.rpcId]) {
                    global.restRpcCallbacks[msg.rpcId](msg);
                    delete global.restRpcCallbacks[msg.rpcId];
                }
            }
        } catch (e) {}
    });

    ws.on("close", () => {
        console.log(`[Hub] Edge Node ${nodeId} disconnected from ${channel}`);
        if (channel === "lavalink") node.wsLavalink = null;
        if (channel === "rest") node.wsRest = null;
        if (!node.wsLavalink && !node.wsRest) edgeNodes.delete(nodeId);
    });
});

// HANDLE BOT CONNECTIONS (Inbound from Discord Bots)
server.on("upgrade", (request, socket, head) => {
    if (request.url === "/v4/websocket") {
        const userId = request.headers["user-id"];
        if (!userId) { socket.destroy(); return; }

        wss.handleUpgrade(request, socket, head, (ws) => {
            ws.isBot = true;
            ws.botUserId = userId;
            ws.botHeaders = request.headers;
            wss.emit("connection", ws, request);
        });
    }
});

wss.on("connection", (ws) => {
    if (!ws.isBot) return;

    const userId = ws.botUserId;
    // Simple load balancing: pick node with least players
    const targetNode = [...edgeNodes.entries()]
        .filter(([_, n]) => n.wsLavalink && n.wsRest)
        .sort((a, b) => (a[1].stats.playingPlayers || 0) - (b[1].stats.playingPlayers || 0))[0];

    if (!targetNode) {
        ws.close(4000, "No edge nodes available");
        return;
    }

    const [nodeId, node] = targetNode;
    botSessions.set(userId, nodeId);
    console.log(`[Hub] Bot ${userId} routed to Edge Node ${nodeId}`);

    // Tell Edge Node to open a local connection to Lavalink for this Bot
    node.wsLavalink.send(JSON.stringify({
        op: "new_session",
        userId: userId,
        clientName: ws.botHeaders["client-name"] || "Unknown",
        resumeKey: ws.botHeaders["resume-key"]
    }));

    // Bridge Bot WS <-> Edge Node WS
    const edgeHandler = (data) => ws.readyState === WebSocket.OPEN && ws.send(data);
    node.wsLavalink.on("message", edgeHandler);
    
    ws.on("message", (data) => {
        if (node.wsLavalink.readyState === WebSocket.OPEN) node.wsLavalink.send(data);
    });

    ws.on("close", () => {
        node.wsLavalink.off("message", edgeHandler);
        botSessions.delete(userId);
        // Tell edge node to close local session
        if(node.wsLavalink.readyState === WebSocket.OPEN) {
            node.wsLavalink.send(JSON.stringify({ op: "destroy_session", userId }));
        }
    });
});

// REST API PROXY (Bot -> Hub -> Edge Node -> Local Lavalink)
global.restRpcCallbacks = {};
let rpcCounter = 0;

app.all("/v4/*", async (req, res) => {
    const userId = req.headers["user-id"];
    const nodeId = botSessions.get(userId);

    if (!nodeId) return res.status(400).json({ error: "Bot session not found" });
    
    const node = edgeNodes.get(nodeId);
    if (!node || !node.wsRest || node.wsRest.readyState !== WebSocket.OPEN) {
        return res.status(502).json({ error: "Edge node REST channel unavailable" });
    }

    const rpcId = ++rpcCounter;
    
    // Send RPC request to Edge Node
    node.wsRest.send(JSON.stringify({
        rpcId,
        method: req.method,
        path: req.originalUrl,
        headers: { 
            authorization: req.headers["authorization"],
            session_id: req.headers["session-id"]
        },
        body: req.body
    }));

    // Wait for Edge Node to reply
    const timeout = setTimeout(() => {
        delete global.restRpcCallbacks[rpcId];
        res.status(504).json({ error: "Edge node REST timeout" });
    }, 10000);

    global.restRpcCallbacks[rpcId] = (response) => {
        clearTimeout(timeout);
        res.status(response.status).json(response.body);
    };
});

const PORT = process.env?.PORT || 2333;
server.listen(PORT, () => console.log(`🚀 Hub listening on port ${PORT}`));