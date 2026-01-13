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
  // ✅ FIX: Allow Polling to stabilize Render connections
  transports: ['polling', 'websocket']
});

// ✅ STATE 2: Simple, Reliable Storage
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
      
      console.log(`👑 Host Registered: ${username}`);
      broadcastToHost(roomId);
  });

  // 2️⃣ VIEWER JOINS (Callback Pattern)
  socket.on('join-room', ({ roomId, userId, username }, callback) => {
      socket.join(roomId);

      if (!rooms[roomId]) rooms[roomId] = { users: [] };
      
      // Add User (Avoid Duplicates)
      const existingUser = rooms[roomId].users.find(u => u.username === username);
      if (!existingUser) {
          rooms[roomId].users.push({ socketId: socket.id, username, status: 'LIVE' });
      } else {
          existingUser.socketId = socket.id; // Update socket if reconnecting
      }

      console.log(`👤 Viewer Joined: ${username}`);

      // Update Host UI
      broadcastToHost(roomId);
      
      // Trigger Video (Only if userId exists)
      if (userId) {
          socket.to(roomId).emit('user-connected', userId);
      }

      // ✅ IMMEDIATE RESPONSE: Send Host Name to Viewer
      if (callback) {
          callback({
              hostName: rooms[roomId].hostName || "Party's Room"
          });
      }
  });

  // 3️⃣ CHAT
  socket.on('send-message', (data) => {
      socket.to(data.roomId).emit('receive-message', data);
  });

  // 4️⃣ SYNC
  socket.on('video-sync', (data) => {
      if (data.targetSocketId) {
          io.to(data.targetSocketId).emit('video-sync', data);
      } else {
          socket.to(data.roomId).emit('video-sync', data);
      }
  });

  socket.on('request-sync', (roomId) => {
      if(rooms[roomId]?.hostSocket) {
          io.to(rooms[roomId].hostSocket).emit('request-sync-from-host', socket.id);
      }
  });

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
