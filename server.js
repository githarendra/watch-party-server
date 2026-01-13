const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');

const app = express();
const server = http.createServer(app);

// ✅ Allow all origins
app.use(cors({ origin: "*", credentials: true }));

const peerServer = ExpressPeerServer(server, { debug: true, path: "/", allow_discovery: true });
app.use("/peerjs", peerServer);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  transports: ['polling', 'websocket'] // ✅ Fixes Render Connection
});

// ✅ STORAGE
const rooms = {}; // { roomId: { hostSocket, hostName, users: [] } }

const broadcastToHost = (roomId) => {
    if (rooms[roomId] && rooms[roomId].hostSocket) {
        io.to(rooms[roomId].hostSocket).emit('update-user-list', rooms[roomId].users);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Socket Connected:", socket.id);

  // 1️⃣ HOST REGISTERS
  socket.on('register-host', ({ roomId, username }) => {
      socket.join(roomId);
      
      if (!rooms[roomId]) rooms[roomId] = { users: [] };
      rooms[roomId].hostSocket = socket.id;
      rooms[roomId].hostName = username;
      
      console.log(`👑 Host Registered: ${username} (${roomId})`);
      
      // Update anyone waiting
      socket.to(roomId).emit('host-name-update', username);
      broadcastToHost(roomId);
  });

  // 2️⃣ VIEWER JOINS DATA (Immediate - Fixes Name/Chat/Count)
  socket.on('join-room-data', ({ roomId, username }, callback) => {
      socket.join(roomId);

      if (!rooms[roomId]) rooms[roomId] = { users: [] };
      
      // Add User to List (No Peer ID yet)
      const existingUser = rooms[roomId].users.find(u => u.username === username);
      if (!existingUser) {
          rooms[roomId].users.push({ socketId: socket.id, username, status: 'Connecting...' });
      } else {
          existingUser.socketId = socket.id; // Update socket
      }

      console.log(`👤 Viewer Connected Data: ${username}`);

      // Update Host Count
      broadcastToHost(roomId);

      // ✅ SEND HOST NAME IMMEDIATELY (Via Callback)
      if (callback) {
          callback({ hostName: rooms[roomId].hostName || "Party's Room" });
      }
  });

  // 3️⃣ VIEWER UPGRADES TO VIDEO (Background)
  socket.on('update-peer-id', ({ roomId, peerId }) => {
      // Tell Host to Call this Peer
      socket.to(roomId).emit('user-connected', peerId);
      
      // Update Status to LIVE
      if(rooms[roomId]) {
          const user = rooms[roomId].users.find(u => u.socketId === socket.id);
          if (user) {
              user.status = 'LIVE';
              broadcastToHost(roomId);
          }
      }
  });

  // 4️⃣ SYNC & CHAT
  socket.on('request-sync', (roomId) => {
      if(rooms[roomId]?.hostSocket) {
          io.to(rooms[roomId].hostSocket).emit('request-sync-from-host', socket.id);
      }
  });

  socket.on('video-sync', (data) => {
    if (data.targetSocketId) io.to(data.targetSocketId).emit('video-sync', data);
    else socket.to(data.roomId).emit('video-sync', data);
  });

  socket.on('send-message', (data) => socket.to(data.roomId).emit('receive-message', data));

  socket.on('viewer-status-update', ({ roomId, status }) => {
      if (rooms[roomId]) {
          const user = rooms[roomId].users.find(u => u.socketId === socket.id);
          if(user) { user.status = status; broadcastToHost(roomId); }
      }
  });

  socket.on('stop-broadcast', (roomId) => {
      if(rooms[roomId]) delete rooms[roomId].hostSocket;
      socket.to(roomId).emit('broadcast-stopped');
  });

  socket.on('disconnect', () => {
      for (const roomId in rooms) {
          const idx = rooms[roomId].users.findIndex(u => u.socketId === socket.id);
          if (idx !== -1) {
              rooms[roomId].users.splice(idx, 1);
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
