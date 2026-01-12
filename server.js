const express = require('express');
const http = require('http');
const { Server } = require("socket.io");
const { ExpressPeerServer } = require("peer");
const cors = require('cors');

const app = express();
const server = http.createServer(app);

// ✅ Your Render URL
const CLIENT_URL = "https://client-six-vert-25.vercel.app"; 

app.use(cors({ origin: CLIENT_URL, credentials: true }));

const peerServer = ExpressPeerServer(server, { debug: true, path: "/", allow_discovery: true });
app.use("/peerjs", peerServer);

const io = new Server(server, {
  cors: { origin: CLIENT_URL, methods: ["GET", "POST"], credentials: true }
});

const roomHosts = {}; 
const roomUsers = {}; 
const socketRoomMap = {}; 
const roomDetails = {}; // ✅ Stores Host Name

// Helper to broadcast updated user list to Host
const broadcastToHost = (roomId) => {
    const hostSocketId = roomHosts[roomId];
    if (hostSocketId && roomUsers[roomId]) {
        io.to(hostSocketId).emit('update-user-list', roomUsers[roomId]);
    }
};

io.on('connection', (socket) => {
  console.log("✅ Connected:", socket.id);

  // 1️⃣ Host Registers (On Page Load)
  socket.on('register-host', ({ roomId, username }) => {
      socket.join(roomId);
      roomHosts[roomId] = socket.id;
      socketRoomMap[socket.id] = roomId;
      roomDetails[roomId] = { hostName: username };
      
      console.log(`🏠 Host registered: ${username} in ${roomId}`);

      // If viewers are already there, update them
      socket.to(roomId).emit('host-name-update', username);
      
      // Update Host with current list (if any viewers are waiting)
      broadcastToHost(roomId);
  });

  // 2️⃣ Viewer Joins
  socket.on('join-room', (roomId, userId, username) => {
    socket.join(roomId);
    socketRoomMap[socket.id] = roomId;

    if (!roomUsers[roomId]) roomUsers[roomId] = [];
    
    // Remove duplicates
    roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socket.id);
    
    // Add User
    roomUsers[roomId].push({ socketId: socket.id, username, status: 'LIVE' });

    console.log(`👤 Viewer joined: ${username} in ${roomId}`);

    // Notify Host immediately
    broadcastToHost(roomId);
    socket.to(roomId).emit('user-connected', userId);

    // ✅ SEND HOST NAME TO NEW VIEWER
    if (roomDetails[roomId]) {
        socket.emit('host-name-update', roomDetails[roomId].hostName);
    }
  });

  // 3️⃣ Host Starts Stream (Update Name & List)
  socket.on('host-started-stream', ({ roomId, username }) => {
    roomHosts[roomId] = socket.id;
    roomDetails[roomId] = { hostName: username };
    
    socket.to(roomId).emit('stream-forced-refresh');
    socket.to(roomId).emit('host-name-update', username);
    broadcastToHost(roomId);
  });

  // 4️⃣ Sync Request (Viewer asks for time)
  socket.on('request-sync', (roomId) => {
      const hostSocketId = roomHosts[roomId];
      if (hostSocketId) {
          io.to(hostSocketId).emit('request-sync-from-host', socket.id);
      }
  });

  // 5️⃣ Viewer Status Update (Play/Pause)
  socket.on('viewer-status-update', ({ roomId, status }) => {
      if (roomUsers[roomId]) {
          const user = roomUsers[roomId].find(u => u.socketId === socket.id);
          if (user) {
              user.status = status;
              broadcastToHost(roomId);
          }
      }
  });

  // Standard Events
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
      if (roomUsers[roomId]) {
          roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socketId);
      }
      broadcastToHost(roomId);
  });

  socket.on('stop-broadcast', (roomId) => {
    delete roomHosts[roomId];
    delete roomDetails[roomId];
    socket.to(roomId).emit('broadcast-stopped');
  });

  socket.on('disconnect', () => {
    const roomId = socketRoomMap[socket.id];
    if (roomId) {
        // If Viewer Left
        if (roomUsers[roomId]) {
            roomUsers[roomId] = roomUsers[roomId].filter(u => u.socketId !== socket.id);
            broadcastToHost(roomId);
        }
        // If Host Left
        if (roomHosts[roomId] === socket.id) {
            io.to(roomId).emit('broadcast-stopped'); 
            delete roomHosts[roomId];
            delete roomDetails[roomId];
        }
        delete socketRoomMap[socket.id];
    }
  });
});

server.listen(3001, () => {
  console.log('🚀 Server running on port 3001');
});
