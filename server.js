const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');

const app = express();
const server = http.createServer(app);

// ✅ Allow Render/Vercel connections
app.use(cors({ origin: "*", credentials: true }));

const peerServer = ExpressPeerServer(server, { debug: true, path: "/", allow_discovery: true });
app.use("/peerjs", peerServer);

const io = new Server(server, {
  cors: { 
    origin: "*", 
    methods: ["GET", "POST"], 
    credentials: true 
  },
  // ✅ FIX: 'polling' first fixes the "WebSocket failed" error on Render
  transports: ['polling', 'websocket'] 
});

// ✅ CENTRAL STORAGE (The "Old" Reliable Approach)
const rooms = {}; // Structure: { roomId: { hostSocket, hostName, users: [] } }

const broadcastToHost = (roomId) => {
    if (rooms[roomId] && rooms[roomId].hostSocket) {
        io.to(rooms[roomId].hostSocket).emit('update-user-list', rooms[roomId].users);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Socket Connected:", socket.id);

  // 1️⃣ HOST REGISTERS (Run this on load AND reconnect)
  socket.on('register-host', ({ roomId, username }) => {
      socket.join(roomId);
      
      if (!rooms[roomId]) rooms[roomId] = { users: [] };
      
      // ✅ Update the Host's Socket ID immediately so they get updates
      rooms[roomId].hostSocket = socket.id;
      rooms[roomId].hostName = username;
      
      console.log(`👑 Host Registered: ${username} (Room: ${roomId})`);
      
      // Send the list back to the host immediately
      broadcastToHost(roomId);
  });

  // 2️⃣ VIEWER JOINS
  socket.on('join-room', (roomId, userId, username) => {
    socket.join(roomId);

    if (!rooms[roomId]) rooms[roomId] = { users: [] };
    
    // Prevent duplicates
    rooms[roomId].users = rooms[roomId].users.filter(u => u.socketId !== socket.id);
    
    // Add Viewer to List
    rooms[roomId].users.push({ socketId: socket.id, username, status: 'LIVE' });

    console.log(`👤 Viewer Joined: ${username}`);

    // ✅ UPDATE HOST DASHBOARD
    broadcastToHost(roomId);
    
    // ✅ TRIGGER VIDEO CONNECTION
    socket.to(roomId).emit('user-connected', userId);

    // ✅ SEND HOST NAME TO VIEWER
    if (rooms[roomId].hostName) {
        socket.emit('host-name', rooms[roomId].hostName);
    }
  });

  // 3️⃣ HOST STARTS STREAM
  socket.on('host-started-stream', ({ roomId, username }) => {
    if (!rooms[roomId]) rooms[roomId] = { users: [] };
    
    rooms[roomId].hostSocket = socket.id;
    rooms[roomId].hostName = username;
    
    socket.to(roomId).emit('stream-forced-refresh');
    socket.to(roomId).emit('host-name', username); // Update existing viewers
    broadcastToHost(roomId);
  });

  // 4️⃣ SYNC REQUEST
  socket.on('request-sync', (roomId) => {
      if (rooms[roomId] && rooms[roomId].hostSocket) {
          io.to(rooms[roomId].hostSocket).emit('request-sync-from-host', socket.id);
      }
  });

  // 5️⃣ STATUS UPDATE (Watching/Paused)
  socket.on('viewer-status-update', ({ roomId, status }) => {
      if (rooms[roomId]) {
          const user = rooms[roomId].users.find(u => u.socketId === socket.id);
          if (user) {
              user.status = status;
              broadcastToHost(roomId);
          }
      }
  });

  // General Events
  socket.on('send-message', (data) => socket.to(data.roomId).emit('receive-message', data));
  
  socket.on('video-sync', (data) => {
    if (data.targetSocketId) {
        io.to(data.targetSocketId).emit('video-sync', data);
    } else {
        socket.to(data.roomId).emit('video-sync', data);
    }
  });

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
              broadcastToHost(roomId); // Update Host immediately
              break;
          }
      }
  });
});

const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
  console.log(`🚀 Server running on port ${PORT}`);
});
