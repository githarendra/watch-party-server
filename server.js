const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');

const app = express();
const server = http.createServer(app);

// ✅ Allow all origins to prevent connection blocking
app.use(cors());

const peerServer = ExpressPeerServer(server, { debug: true, path: "/", allow_discovery: true });
app.use("/peerjs", peerServer);

const io = new Server(server, {
  cors: { 
    origin: "*", 
    methods: ["GET", "POST"], 
    credentials: true 
  },
  // ✅ FIX: Force polling first (most compatible), then upgrade
  transports: ['polling', 'websocket'] 
});

// STABLE STORAGE
const roomHosts = {};      // roomId -> socketId
const roomNames = {};      // roomId -> "Harry's Room"
const roomUsers = {};      // roomId -> Array of users

const broadcastToHost = (roomId) => {
    const hostSocket = roomHosts[roomId];
    if (hostSocket && roomUsers[roomId]) {
        io.to(hostSocket).emit('update-user-list', roomUsers[roomId]);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Connected:", socket.id);

  // 1️⃣ HOST REGISTERS
  socket.on('register-host', ({ roomId, username }) => {
      socket.join(roomId);
      roomHosts[roomId] = socket.id;
      roomNames[roomId] = username;
      
      if (!roomUsers[roomId]) roomUsers[roomId] = [];
      
      // Send name to anyone already there
      socket.to(roomId).emit('host-name-update', username);
      broadcastToHost(roomId);
  });

  // 2️⃣ VIEWER JOINS
  socket.on('join-room', (roomId, userId, username) => {
    socket.join(roomId);

    if (!roomUsers[roomId]) roomUsers[roomId] = [];
    
    // Remove duplicates
    roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socket.id);
    
    // Add User
    roomUsers[roomId].push({ socketId: socket.id, username, status: 'LIVE' });

    // Update Host
    broadcastToHost(roomId);
    
    // Trigger Video Call
    socket.to(roomId).emit('user-connected', userId);

    // ✅ Send Host Name immediately
    if (roomNames[roomId]) {
        socket.emit('host-name-update', roomNames[roomId]);
    }
  });

  // 3️⃣ FETCH NAME (Redundancy)
  socket.on('get-host-name', (roomId) => {
      if (roomNames[roomId]) {
          socket.emit('host-name-update', roomNames[roomId]);
      }
  });

  socket.on('host-started-stream', ({ roomId, username }) => {
    roomHosts[roomId] = socket.id;
    roomNames[roomId] = username;
    socket.to(roomId).emit('stream-forced-refresh');
    socket.to(roomId).emit('host-name-update', username);
    broadcastToHost(roomId);
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

  // Sync Handshake
  socket.on('request-sync', (roomId) => {
      const hostSocket = roomHosts[roomId];
      if (hostSocket) {
          io.to(hostSocket).emit('request-sync-from-host', socket.id);
      }
  });

  socket.on('video-sync', (data) => {
    if (data.targetSocketId) {
        io.to(data.targetSocketId).emit('video-sync', data);
    } else {
        socket.to(data.roomId).emit('video-sync', data);
    }
  });

  socket.on('send-message', (data) => socket.to(data.roomId).emit('receive-message', data));

  socket.on('kick-user', ({ roomId, socketId }) => {
      io.to(socketId).emit('kicked');
      if (roomUsers[roomId]) {
          roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socketId);
      }
      broadcastToHost(roomId);
  });

  socket.on('stop-broadcast', (roomId) => {
    delete roomHosts[roomId];
    delete roomNames[roomId];
    socket.to(roomId).emit('broadcast-stopped');
  });

  socket.on('disconnect', () => {
      // Cleanup logic
      for (const roomId in roomUsers) {
          const index = roomUsers[roomId].findIndex(u => u.socketId === socket.id);
          if (index !== -1) {
              roomUsers[roomId].splice(index, 1);
              broadcastToHost(roomId);
              break;
          }
          if (roomHosts[roomId] === socket.id) {
              // Host left
              break;
          }
      }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
