const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');

const app = express();
const server = http.createServer(app);

// ✅ Allow all connections
app.use(cors({ origin: "*", credentials: true }));

const peerServer = ExpressPeerServer(server, { debug: true, path: "/", allow_discovery: true });
app.use("/peerjs", peerServer);

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
  transports: ['polling', 'websocket'] // ✅ Fixes Render Connection
});

// ✅ Simple, Reliable Storage
const rooms = {}; // roomId -> { hostSocket, hostName, users: [] }

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
      
      // Update Host UI immediately
      broadcastToHost(roomId);
  });

  // 2️⃣ VIEWER JOINS (Standard Logic)
  socket.on('join-room', ({ roomId, userId, username }, callback) => {
      socket.join(roomId);

      if (!rooms[roomId]) rooms[roomId] = { users: [] };
      
      // Add User
      const existingUser = rooms[roomId].users.find(u => u.socketId === socket.id);
      if (!existingUser) {
          rooms[roomId].users.push({ socketId: socket.id, username, status: 'LIVE' });
      }

      console.log(`👤 Viewer Joined: ${username}`);

      // A. Update Host Dashboard
      broadcastToHost(roomId);
      
      // B. Tell Host to Call this Peer
      socket.to(roomId).emit('user-connected', userId);

      // C. Send Host Name to Viewer (IMMEDIATELY)
      if (callback) {
          callback({ hostName: rooms[roomId].hostName || "Party's Room" });
      }
  });

  // 3️⃣ CHAT (Standard Room Emit)
  socket.on('send-message', (data) => {
      // Broadcast to everyone in the room (including sender if needed, but usually sender handles own UI)
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
          if (user) {
              user.status = status;
              broadcastToHost(roomId);
          }
      }
  });

  socket.on('kick-user', ({ roomId, socketId }) => {
      io.to(socketId).emit('kicked');
      if(rooms[roomId]) {
          rooms[roomId].users = rooms[roomId].users.filter(u => u.socketId !== socketId);
          broadcastToHost(roomId);
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
