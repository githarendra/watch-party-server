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
  // ✅ Essential for Render stability
  transports: ['polling', 'websocket'] 
});

// STORAGE
const rooms = {}; // { roomId: { hostSocket, hostName, users: [] } }

const broadcastToHost = (roomId) => {
    if (rooms[roomId] && rooms[roomId].hostSocket) {
        io.to(rooms[roomId].hostSocket).emit('update-user-list', rooms[roomId].users);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Socket Connected:", socket.id);

  // 1️⃣ HOST REGISTERS (Heartbeat)
  socket.on('register-host', ({ roomId, username }) => {
      socket.join(roomId);
      
      if (!rooms[roomId]) rooms[roomId] = { users: [] };
      rooms[roomId].hostSocket = socket.id;
      rooms[roomId].hostName = username;
      
      // Send updates
      broadcastToHost(roomId);
      socket.to(roomId).emit('host-name-update', username);
  });

  // 2️⃣ VIEWER JOINS (Callback Pattern)
  socket.on('join-room', ({ roomId, username }, callback) => {
      socket.join(roomId);

      if (!rooms[roomId]) rooms[roomId] = { users: [] };
      
      // Update User List
      const existingUser = rooms[roomId].users.find(u => u.socketId === socket.id);
      if (!existingUser) {
          rooms[roomId].users.push({ socketId: socket.id, username, status: 'LIVE' });
      }

      console.log(`👤 Viewer ${username} joined ${roomId}`);

      // Update Host UI
      broadcastToHost(roomId);

      // ✅ IMMEDIATE RESPONSE: Send Host Name back to Viewer
      if (callback) {
          callback({
              hostName: rooms[roomId].hostName || "Party's Room"
          });
      }
  });

  // 3️⃣ VIDEO CONNECTION REQUEST
  socket.on('request-video-connection', ({ roomId, peerId }) => {
      socket.to(roomId).emit('user-connected', peerId);
  });

  // 4️⃣ HEARTBEAT (Keep Alive)
  socket.on('heartbeat', ({ roomId, username, role }) => {
      if (!rooms[roomId]) return;
      
      if (role === 'host') {
          rooms[roomId].hostSocket = socket.id;
          rooms[roomId].hostName = username;
      } else {
          const user = rooms[roomId].users.find(u => u.username === username);
          if (user) user.socketId = socket.id; // Refresh ID
      }
      broadcastToHost(roomId);
  });

  // Standard Sync/Messaging
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
      // Basic cleanup - Heartbeat handles re-adds
      for (const roomId in rooms) {
          const idx = rooms[roomId].users.findIndex(u => u.socketId === socket.id);
          if (idx !== -1) {
              rooms[roomId].users.splice(idx, 1);
              broadcastToHost(roomId);
          }
      }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
