const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');

const app = express();
const server = http.createServer(app);

app.use(cors({ origin: "*", credentials: true }));

const peerServer = ExpressPeerServer(server, { debug: true, path: "/", allow_discovery: true });
app.use("/peerjs", peerServer);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  transports: ['polling', 'websocket'] // Keep this for Render stability
});

// ✅ Simple, Stable Storage
const roomHosts = {};      // roomId -> socketId
const roomHostNames = {};  // roomId -> "Harry"
const roomUsers = {};      // roomId -> [users]

const broadcastToHost = (roomId) => {
    const hostSocketId = roomHosts[roomId];
    if (hostSocketId && roomUsers[roomId]) {
        io.to(hostSocketId).emit('update-user-list', roomUsers[roomId]);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Connected:", socket.id);

  // 1️⃣ HOST REGISTERS (Run on load & reconnect)
  socket.on('register-host', ({ roomId, username }) => {
      socket.join(roomId);
      roomHosts[roomId] = socket.id;       // Update ID
      roomHostNames[roomId] = username;    // Store Name
      
      if (!roomUsers[roomId]) roomUsers[roomId] = [];
      
      console.log(`👑 Host Registered: ${username}`);
      
      // Update Host UI immediately
      broadcastToHost(roomId);
      // Tell everyone the name
      socket.to(roomId).emit('host-name-update', username);
  });

  // 2️⃣ VIEWER JOINS
  socket.on('join-room', (roomId, userId, username) => {
      socket.join(roomId);

      if (!roomUsers[roomId]) roomUsers[roomId] = [];
      
      // Remove duplicates
      roomUsers[roomId] = roomUsers[roomId].filter(u => u.username !== username && u.socketId !== socket.id);
      
      // Add User
      roomUsers[roomId].push({ socketId: socket.id, username, status: 'LIVE' });

      console.log(`👤 Viewer Joined: ${username}`);

      // Update Host UI
      broadcastToHost(roomId);
      
      // Tell Host to call Viewer (PeerJS)
      socket.to(roomId).emit('user-connected', userId);

      // Send Host Name to Viewer
      if (roomHostNames[roomId]) {
          socket.emit('host-name-update', roomHostNames[roomId]);
      }
  });

  // 3️⃣ SYNC & STATUS
  socket.on('request-sync', (roomId) => {
      const hostSocketId = roomHosts[roomId];
      if (hostSocketId) {
          io.to(hostSocketId).emit('request-sync-from-host', socket.id);
      }
  });

  socket.on('video-sync', (data) => {
    if (data.targetSocketId) io.to(data.targetSocketId).emit('video-sync', data);
    else socket.to(data.roomId).emit('video-sync', data);
  });

  socket.on('viewer-status-update', ({ roomId, status }) => {
      if (roomUsers[roomId]) {
          const user = roomUsers[roomId].find(u => u.socketId === socket.id);
          if (user) {
              user.status = status;
              broadcastToHost(roomId);
          }
      }
  });

  socket.on('send-message', (data) => socket.to(data.roomId).emit('receive-message', data));

  socket.on('kick-user', ({ roomId, socketId }) => {
      io.to(socketId).emit('kicked');
      if (roomUsers[roomId]) {
          roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socketId);
          broadcastToHost(roomId);
      }
  });

  socket.on('stop-broadcast', (roomId) => {
      delete roomHosts[roomId];
      delete roomHostNames[roomId];
      socket.to(roomId).emit('broadcast-stopped');
  });

  socket.on('disconnect', () => {
      // Find room and cleanup
      for (const roomId in roomUsers) {
          const idx = roomUsers[roomId].findIndex(u => u.socketId === socket.id);
          if (idx !== -1) {
              roomUsers[roomId].splice(idx, 1);
              broadcastToHost(roomId);
              break;
          }
      }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
