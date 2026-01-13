const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');

const app = express();
const server = http.createServer(app);

// ✅ Allow all origins (Fixes CORS issues)
app.use(cors({ origin: "*", credentials: true }));

const peerServer = ExpressPeerServer(server, { debug: true, path: "/", allow_discovery: true });
app.use("/peerjs", peerServer);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  // ✅ Standard, stable transports
  transports: ['polling', 'websocket']
});

// ✅ STATE 2: Simple Arrays (Reliable)
const rooms = {}; // { roomId: { hostSocket, hostName, users: [] } }

const broadcastToHost = (roomId) => {
    if (rooms[roomId] && rooms[roomId].hostSocket) {
        io.to(rooms[roomId].hostSocket).emit('update-user-list', rooms[roomId].users);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Socket Connected:", socket.id);

  // 1️⃣ HOST REGISTERS (Immediate)
  socket.on('register-host', ({ roomId, username }) => {
      socket.join(roomId);
      if (!rooms[roomId]) rooms[roomId] = { users: [] };
      
      rooms[roomId].hostSocket = socket.id;
      rooms[roomId].hostName = username;
      
      console.log(`👑 Host Registered: ${username}`);
      
      // Send updates
      socket.to(roomId).emit('host-name-update', username);
      broadcastToHost(roomId);
  });

  // 2️⃣ VIEWER JOINS DATA CHANNEL (Immediate - Fixes Chat/Name)
  socket.on('join-room-data', ({ roomId, username }) => {
      socket.join(roomId);
      if (!rooms[roomId]) rooms[roomId] = { users: [] };

      // Add to list if not exists
      if (!rooms[roomId].users.some(u => u.socketId === socket.id)) {
          rooms[roomId].users.push({ socketId: socket.id, username, status: 'Connecting...' });
      }

      console.log(`👤 Viewer Connected to Data: ${username}`);

      // Send Host Name to Viewer
      if (rooms[roomId].hostName) {
          socket.emit('host-name-update', rooms[roomId].hostName);
      }
      
      // Update Host
      broadcastToHost(roomId);
  });

  // 3️⃣ VIEWER JOINS VIDEO CHANNEL (Background)
  socket.on('join-room-video', ({ roomId, peerId }) => {
      // Update status to LIVE
      if (rooms[roomId]) {
          const user = rooms[roomId].users.find(u => u.socketId === socket.id);
          if (user) {
              user.status = 'LIVE';
              broadcastToHost(roomId);
          }
      }
      // Tell Host to call this Peer
      socket.to(roomId).emit('user-connected', peerId);
  });

  // 4️⃣ SYNC
  socket.on('request-sync', (roomId) => {
      if (rooms[roomId]?.hostSocket) {
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
