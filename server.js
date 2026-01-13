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
  cors: { 
    origin: "*", 
    methods: ["GET", "POST"], 
    credentials: true 
  },
  transports: ['polling', 'websocket'] // ✅ Compatible Mode
});

// ✅ "State 2" Simple Storage
const roomHosts = {};      // roomId -> hostSocketId
const roomUsers = {};      // roomId -> Array of users
const roomHostNames = {};  // ✅ Stores Host Name securely
const socketRoomMap = {}; 

const broadcastToHost = (roomId) => {
    const hostSocketId = roomHosts[roomId];
    if (hostSocketId && roomUsers[roomId]) {
        io.to(hostSocketId).emit('update-user-list', roomUsers[roomId]);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Connected:", socket.id);

  // 1️⃣ HOST REGISTERS
  socket.on('register-host', ({ roomId, username }) => {
      socket.join(roomId);
      roomHosts[roomId] = socket.id;
      roomHostNames[roomId] = username; // Store Name
      socketRoomMap[socket.id] = roomId;
      
      // Init Array
      if (!roomUsers[roomId]) roomUsers[roomId] = [];

      console.log(`👑 Host Registered: ${username}`);
      
      // Broadcast name to room
      socket.to(roomId).emit('host-name', username);
      broadcastToHost(roomId);
  });

  // 2️⃣ VIEWER JOINS
  socket.on('join-room', (roomId, userId, username) => {
    socket.join(roomId);
    socketRoomMap[socket.id] = roomId;

    if (!roomUsers[roomId]) roomUsers[roomId] = [];
    
    // Remove duplicates
    roomUsers[roomId] = roomUsers[roomId].filter(u => u.username !== username);
    
    // Add User
    roomUsers[roomId].push({ socketId: socket.id, username, status: 'LIVE' });

    console.log(`👤 Viewer Joined: ${username}`);

    broadcastToHost(roomId);
    
    // If Peer ID is provided, tell Host to call
    if (userId) {
        socket.to(roomId).emit('user-connected', userId);
    }

    // ✅ SEND HOST NAME IMMEDIATELY
    if (roomHostNames[roomId]) {
        socket.emit('host-name', roomHostNames[roomId]);
    }
  });

  // 3️⃣ VIEWER STATUS (Updates "Watching/Paused")
  socket.on('viewer-status-update', ({ roomId, status }) => {
      if (roomUsers[roomId]) {
          const user = roomUsers[roomId].find(u => u.socketId === socket.id);
          if (user) {
              user.status = status;
              broadcastToHost(roomId);
          }
      }
  });

  // 4️⃣ SYNC
  socket.on('request-sync', (roomId) => {
      const hostSocketId = roomHosts[roomId];
      if (hostSocketId) {
          io.to(hostSocketId).emit('request-sync-from-host', socket.id);
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
    delete roomHostNames[roomId];
    socket.to(roomId).emit('broadcast-stopped');
  });

  socket.on('disconnect', () => {
    const roomId = socketRoomMap[socket.id];
    if (roomId) {
        if (roomUsers[roomId]) {
            roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socket.id);
            broadcastToHost(roomId);
        }
        delete socketRoomMap[socket.id];
    }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
