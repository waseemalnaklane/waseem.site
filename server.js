const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

let broadcasterSocket = null;
const viewers = new Map();
let nextViewerId = 1;
const chatHistory = [];
const MAX_CHAT_HISTORY = 100;

app.use(express.static(__dirname));

const broadcastToAll = (message) => {
  const payload = JSON.stringify(message);
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  });
};

const sendViewerCountToBroadcaster = () => {
  if (!broadcasterSocket || broadcasterSocket.readyState !== WebSocket.OPEN) return;
  broadcasterSocket.send(JSON.stringify({ type: 'viewer-count', count: viewers.size }));
};

wss.on('connection', (socket) => {
  socket.isAlive = true;
  socket.on('pong', () => {
    socket.isAlive = true;
  });

  socket.on('message', (message) => {
    let data;
    try {
      data = JSON.parse(message);
    } catch (error) {
      return;
    }

    switch (data.type) {
      case 'broadcaster':
        broadcasterSocket = socket;
        socket.role = 'broadcaster';
        socket.send(JSON.stringify({ type: 'broadcaster-ready' }));
        broadcastToAll({ type: 'broadcast-status', active: true });
        sendViewerCountToBroadcaster();
        break;

      case 'watcher': {
        const viewerId = nextViewerId++;
        socket.role = 'viewer';
        socket.viewerId = viewerId;
        viewers.set(viewerId, socket);
        if (broadcasterSocket && broadcasterSocket.readyState === WebSocket.OPEN) {
          broadcasterSocket.send(JSON.stringify({ type: 'watcher-join', viewerId }));
          sendViewerCountToBroadcaster();
        } else {
          socket.send(JSON.stringify({ type: 'no-broadcaster' }));
        }
        socket.send(JSON.stringify({ type: 'chat-history', history: chatHistory }));
        break;
      }

      case 'offer':
        if (socket.role === 'broadcaster') {
          const viewer = viewers.get(data.viewerId);
          if (viewer && viewer.readyState === WebSocket.OPEN) {
            viewer.send(JSON.stringify({ type: 'watcher-offer', sdp: data.sdp, viewerId: data.viewerId }));
          }
        }
        break;

      case 'answer':
        if (socket.role === 'viewer') {
          if (broadcasterSocket && broadcasterSocket.readyState === WebSocket.OPEN) {
            broadcasterSocket.send(JSON.stringify({ type: 'watcher-answer', sdp: data.sdp, viewerId: socket.viewerId }));
          }
        }
        break;

      case 'candidate': {
        const target = data.target === 'broadcaster' ? broadcasterSocket : viewers.get(data.viewerId);
        if (target && target.readyState === WebSocket.OPEN) {
          target.send(JSON.stringify({ type: 'ice-candidate', candidate: data.candidate, viewerId: data.viewerId }));
        }
        break;
      }

      case 'stop-broadcast':
        viewers.forEach((viewer) => {
          if (viewer.readyState === WebSocket.OPEN) {
            viewer.send(JSON.stringify({ type: 'broadcast-ended' }));
          }
        });
        broadcastToAll({ type: 'broadcast-status', active: false });
        break;

      case 'quality':
        if (socket.role === 'viewer') {
          if (broadcasterSocket && broadcasterSocket.readyState === WebSocket.OPEN) {
            broadcasterSocket.send(JSON.stringify({ type: 'quality-request', viewerId: socket.viewerId, quality: data.quality }));
          }
        }
        break;

      case 'chat': {
        const messageRecord = {
          name: data.name?.trim() || 'مشاهد',
          text: data.text?.trim() || '',
          time: new Date().toISOString(),
        };
        chatHistory.push(messageRecord);
        if (chatHistory.length > MAX_CHAT_HISTORY) chatHistory.shift();
        broadcastToAll({ type: 'chat-message', name: messageRecord.name, text: messageRecord.text });
        break;
      }

      default:
        break;
    }
  });

  socket.on('close', () => {
    if (socket.role === 'broadcaster') {
      broadcasterSocket = null;
      viewers.forEach((viewer) => {
        if (viewer.readyState === WebSocket.OPEN) {
          viewer.send(JSON.stringify({ type: 'broadcast-ended' }));
        }
      });
      broadcastToAll({ type: 'broadcast-status', active: false });
    }

    if (socket.role === 'viewer' && socket.viewerId) {
      viewers.delete(socket.viewerId);
      if (broadcasterSocket && broadcasterSocket.readyState === WebSocket.OPEN) {
        broadcasterSocket.send(JSON.stringify({ type: 'viewer-disconnect', viewerId: socket.viewerId }));
        sendViewerCountToBroadcaster();
      }
    }
  });
});

setInterval(() => {
  wss.clients.forEach((socket) => {
    if (!socket.isAlive) {
      return socket.terminate();
    }
    socket.isAlive = false;
    socket.ping();
  });
}, 30000);

const PORT = process.env.PORT || 8080;
server.listen(PORT, () => {
  console.log(`Broadcast server running on http://localhost:${PORT}`);
});
