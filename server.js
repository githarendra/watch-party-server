const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');

const app = express();
const server = http.createServer(app);

// ✅ Allow all origins to prevent CORS blocks
app.use(cors({ origin: "*", credentials: true }));

const peerServer = ExpressPeerServer(server, { debug: true, path: "/", allow_discovery: true });
app.use("/peerjs", peerServer);

const io = new Server(server, {
  cors: { 
    origin: "*", // ✅ Open access for stability
    methods: ["GET", "POST"], 
    credentials: true 
  },
  // ✅ CRITICAL FIX: Force Polling first. This fixes the EIO=4 error.
  transports: ['polling', 'websocket'] 
});

// STABLE STORAGE
const roomHosts = {};      // roomId -> socketId
const roomNames = {};      // roomId -> "Harry's Room"
const roomUsers = {};      // roomId -> Array of user objects

// Helper: Send the updated user list to the Host
const broadcastToHost = (roomId) => {
    const hostSocketId = roomHosts[roomId];
    if (hostSocketId && roomUsers[roomId]) {
        io.to(hostSocketId).emit('update-user-list', roomUsers[roomId]);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Socket Connected:", socket.id);

  // 1️⃣ HOST REGISTRATION (Fixes Host Name Persistence)
  socket.on('register-host', ({ roomId, username }) => {
      socket.join(roomId);
      roomHosts[roomId] = socket.id;
      roomNames[roomId] = username;
      
      // If the room doesn't exist in users map, init it
      if (!roomUsers[roomId]) roomUsers[roomId] = [];
      
      console.log(`👑 Host Registered: ${username} in Room: ${roomId}`);
      
      // If viewers are already waiting, tell them the name now
      socket.to(roomId).emit('host-name-update', username);
      // Update Host Dashboard
      broadcastToHost(roomId);
  });

  // 2️⃣ VIEWER JOIN (Fixes Viewer Count)
  socket.on('join-room', (roomId, userId, username) => {
    socket.join(roomId);

    if (!roomUsers[roomId]) roomUsers[roomId] = [];
    
    // Remove any existing entry for this socket/user to prevent duplicates
    roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socket.id);
    
    // Add the new viewer
    roomUsers[roomId].push({ socketId: socket.id, username, status: 'Connecting...' });

    console.log(`👤 Viewer Joined: ${username} (Count: ${roomUsers[roomId].length})`);

    // ✅ UPDATE HOST: Send new count immediately
    broadcastToHost(roomId);
    
    // ✅ TRIGGER VIDEO: Tell Host to call this user
    socket.to(roomId).emit('user-connected', userId);

    // ✅ UPDATE VIEWER: Send them the Host Name immediately
    if (roomNames[roomId]) {
        socket.emit('host-name-update', roomNames[roomId]);
    }
  });

  // 3️⃣ HOST STARTED STREAM (Re-syncs everything)
  socket.on('host-started-stream', ({ roomId, username }) => {
    roomHosts[roomId] = socket.id;
    roomNames[roomId] = username;
    
    socket.to(roomId).emit('stream-forced-refresh');
    socket.to(roomId).emit('host-name-update', username);
    broadcastToHost(roomId);
  });

  // 4️⃣ VIEWER STATUS (Updates "Watching" vs "Paused")
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
    delete roomNames[roomId];
    socket.to(roomId).emit('broadcast-stopped');
  });

  socket.on('disconnect', () => {
      for (const roomId in roomUsers) {
          // Check if Viewer left
          const index = roomUsers[roomId].findIndex(u => u.socketId === socket.id);
          if (index !== -1) {
              roomUsers[roomId].splice(index, 1);
              broadcastToHost(roomId); // Update Host Count
              break;
          }
          // Check if Host left
          if (roomHosts[roomId] === socket.id) {
              // We don't delete immediately to allow refresh, but we could notify
              break;
          }
      }
  });
});

// ✅ Use process.env.PORT for Render
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
