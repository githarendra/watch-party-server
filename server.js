const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');

const app = express();
const server = http.createServer(app);

const CLIENT_URL = "https://client-six-vert-25.vercel.app"; 

app.use(cors({ origin: CLIENT_URL, credentials: true }));

const peerServer = ExpressPeerServer(server, { debug: true, path: "/", allow_discovery: true });
app.use("/peerjs", peerServer);

const io = new Server(server, {
  cors: { 
    origin: CLIENT_URL, 
    methods: ["GET", "POST"], 
    credentials: true 
  },
  transports: ['polling', 'websocket']
});

// ROBUST STORAGE: { roomId: { hostSocket, hostName, users: [] } }
const rooms = {}; 

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
      
      console.log(`👑 Host Registered: ${username} in ${roomId}`);
      
      // Update anyone waiting
      socket.to(roomId).emit('host-name-update', username);
      broadcastToHost(roomId);
  });

  // 2️⃣ VIEWER JOINS (Standard Stable Logic)
  socket.on('join-room', (roomId, userId, username) => {
    socket.join(roomId);

    if (!rooms[roomId]) rooms[roomId] = { users: [] };
    
    // Add User (avoid duplicates)
    rooms[roomId].users = rooms[roomId].users.filter(u => u.socketId !== socket.id);
    rooms[roomId].users.push({ socketId: socket.id, username, status: 'LIVE' });

    console.log(`👤 Viewer Joined: ${username} (${userId})`);

    // 1. Update Host Dashboard
    broadcastToHost(roomId);
    
    // 2. Tell Host to Call this Peer ID
    socket.to(roomId).emit('user-connected', userId);

    // 3. Send Host Name to Viewer
    if (rooms[roomId].hostName) {
        socket.emit('host-name-update', rooms[roomId].hostName);
    }
  });

  // 3️⃣ SYNC HANDSHAKE
  socket.on('request-sync', (roomId) => {
      if (rooms[roomId] && rooms[roomId].hostSocket) {
          io.to(rooms[roomId].hostSocket).emit('request-sync-from-host', socket.id);
      }
  });

  // 4️⃣ HOST STREAM START (Re-confirm Name)
  socket.on('host-started-stream', ({ roomId, username }) => {
    if (!rooms[roomId]) rooms[roomId] = { users: [] };
    rooms[roomId].hostSocket = socket.id;
    rooms[roomId].hostName = username;
    
    socket.to(roomId).emit('stream-forced-refresh');
    socket.to(roomId).emit('host-name-update', username);
    broadcastToHost(roomId);
  });

  // 5️⃣ STATUS UPDATES
  socket.on('viewer-status-update', ({ roomId, status }) => {
      if (rooms[roomId]) {
          const user = rooms[roomId].users.find(u => u.socketId === socket.id);
          if (user) {
              user.status = status;
              broadcastToHost(roomId);
          }
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
      if (rooms[roomId]) {
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
          const room = rooms[roomId];
          if (room.hostSocket === socket.id) {
              // Host left
              break;
          }
          const userIndex = room.users.findIndex(u => u.socketId === socket.id);
          if (userIndex !== -1) {
              room.users.splice(userIndex, 1);
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
