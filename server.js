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
  // ✅ FIX: Disable polling to fix Render EIO=4 error
  transports: ['websocket'] 
});

// Storage
const roomHosts = {}; // roomId -> hostName
const roomHostIds = {}; // roomId -> socketId

// ✅ Get accurate viewer count directly from Socket.IO engine
const getViewerCount = (roomId) => {
    const room = io.sockets.adapter.rooms.get(roomId);
    return room ? room.size - 1 : 0; // Subtract 1 (The Host)
};

io.on('connection', (socket) => {
  console.log("✅ Socket Connected (WS Only):", socket.id);

  // 1️⃣ HOST REGISTRATION
  socket.on('register-host', ({ roomId, username }) => {
      socket.join(roomId);
      roomHosts[roomId] = username;
      roomHostIds[roomId] = socket.id;
      
      console.log(`👑 Host Registered: ${username} in ${roomId}`);
      
      // Update Host with count
      socket.emit('viewer-count-update', getViewerCount(roomId));
  });

  // 2️⃣ VIEWER JOINS (With Callback for Host Name)
  socket.on('join-room', ({ roomId, username }, callback) => {
      socket.join(roomId);
      
      console.log(`👤 Viewer Joined: ${username}`);

      // Notify Host of new count
      const hostSocket = roomHostIds[roomId];
      if (hostSocket) {
          io.to(hostSocket).emit('viewer-count-update', getViewerCount(roomId));
          io.to(hostSocket).emit('user-connected', socket.id); // Trigger Peer Call
      }

      // ✅ IMMEDIATE CALLBACK: Send Host Name to Viewer
      if (callback) {
          callback({ 
              hostName: roomHosts[roomId] || "Party's Room" 
          });
      }
  });

  // 3️⃣ SYNC RELAY
  socket.on('request-sync', (roomId) => {
      const hostSocket = roomHostIds[roomId];
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

  socket.on('viewer-status-update', ({ roomId, status }) => {
      // Just relay to host for now if needed, or broadcast
      const hostSocket = roomHostIds[roomId];
      if(hostSocket) io.to(hostSocket).emit('viewer-status-update', { id: socket.id, status });
  });

  socket.on('send-message', (data) => socket.to(data.roomId).emit('receive-message', data));

  socket.on('kick-user', ({ roomId, socketId }) => {
      io.to(socketId).emit('kicked');
      // Update count after kick logic (socket leave happens on client disconnect usually)
  });

  socket.on('stop-broadcast', (roomId) => {
      delete roomHosts[roomId];
      delete roomHostIds[roomId];
      socket.to(roomId).emit('broadcast-stopped');
  });

  socket.on('disconnect', () => {
      // Check which rooms this socket was in to update counts
      // socket.rooms is empty on disconnect, so we scan
      for (const roomId in roomHostIds) {
          if (roomHostIds[roomId] === socket.id) {
              // Host disconnected
              io.to(roomId).emit('broadcast-stopped');
              delete roomHosts[roomId];
              delete roomHostIds[roomId];
          } else {
              // Potentially a viewer, update host count
              const hostSocket = roomHostIds[roomId];
              if (hostSocket) {
                  io.to(hostSocket).emit('viewer-count-update', getViewerCount(roomId));
              }
          }
      }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
